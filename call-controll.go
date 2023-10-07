package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"
	"database/sql"
	"net/http"
	_"encoding/json"
	"path/filepath"
	_ "github.com/go-sql-driver/mysql"
	_"io/ioutil"
	"github.com/joho/godotenv"
)

const (
	RecordBaseDir = "/var/spool/asterisk/monitor"
	BucketName = "pbx-recording"
	CallCompanyPath = "audio/call-company"
	CallClientPath = "audio/call-client"
)

var (
	err error
	db *sql.DB
	call  CallInfo
)

type CallInfo struct {
	Uniqueid        string       `json:"uniqueid"`
	Linkedid        string       `json:"linkedid"`
	Disposition     string       `json:"disposition"`
	Duration        string       `json:"duration"`
	Billsec         string       `json:"billsec"`
	Channel         string       `json:"channel"`
	Dstchannel      string       `json:"dstchannel"`
	Lastapp         string       `json:"lastapp"`
	Lastdata        string       `json:"lastdata"`
	Recordingfile   string       `json:"recordingfile"`
}

type handler func(w http.ResponseWriter, r *http.Request)

type RequestData struct {
    Uniqueid string `json:"uniqueid"`
}


func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

func set_call_controll_info(uniqueid string){

	time.Sleep(30 * time.Second)


	call_info := get_call_info(uniqueid)

	full_record_path := get_recording_path(call_info.Recordingfile)
	fmt.Println(call_info.Recordingfile)

	fmt.Println(full_record_path)
	fmt.Println(fmt.Sprintf("%s/%s", full_record_path, get_date_path()))

	
	// save_recoding_to_s3(full_record_path, fmt.Sprintf("%s/%s", CallCompanyPath, get_date_path()))

}

func save_recoding_to_s3(localRecordFilePath string, s3FolderPath string){

    cmd := exec.Command("s3cmd", "put", localRecordFilePath, fmt.Sprintf("s3://%s/%s", BucketName, s3FolderPath))

    output, err := cmd.CombinedOutput()
    if err != nil {
        fmt.Println("Ошибка при выполнении команды s3cmd:", err)
        return
    }

    fmt.Println("Результат выполнения команды s3cmd:")
    fmt.Println(string(output))
}


func get_call_info(uniqueid string) (CallInfo) {

	var call_info CallInfo

	call_data, err := db.Query("select uniqueid, linkedid, disposition, duration, billsec, channel, dstchannel, lastapp, lastdata, recordingfile from cdr where uniqueid like ?", uniqueid)

	checkErr(err)

	defer call_data.Close()

	if call_data.Next() {
		err := call_data.Scan(
			&call_info.Uniqueid,
			&call_info.Linkedid,
			&call_info.Disposition,
			&call_info.Duration,
			&call_info.Billsec,
			&call_info.Channel,
			&call_info.Dstchannel,
			&call_info.Lastapp,
			&call_info.Lastdata,
			&call_info.Recordingfile,
		)
		checkErr(err)

	}

	return call_info
}

func get_recording_path(recordingfile string) string {

    filePath := filepath.Join(RecordBaseDir, get_date_path(), recordingfile)

    return filePath
}

func get_date_path() string {
	now := time.Now()

    datePath := fmt.Sprintf("%d/%02d/%02d/", now.Year(), now.Month(), now.Day())

	return datePath
}

func PostOnly(h handler) handler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			h(w, r)
			return
		}
	}
}

func call_controll_handler(w http.ResponseWriter, r *http.Request){
	r.ParseForm()

    uniqueid := r.FormValue("uniqueid")

	w.WriteHeader(http.StatusOK)

	go func(uniqueid string) {
		set_call_controll_info(uniqueid)
	}(uniqueid)

	return
}

func goDotEnvVariable(key string) string {

	err := godotenv.Load(".env")
  
	if err != nil {
	  log.Fatalf("Error loading .env file")
	}
  
	return os.Getenv(key)
  }
  

func main() {

    connectionString := fmt.Sprintf("%s:%s@(%s:%s)/%s", goDotEnvVariable("MYSQL_USERNAME"), goDotEnvVariable("MYSQL_PASSWORD"), "127.0.0.1", "3306", "asteriskcdrdb")

	db, _ = sql.Open("mysql", connectionString)

	defer db.Close()

	err = db.Ping()

	checkErr(err)

	defer db.Close()

	http.HandleFunc("/call-controll", PostOnly(call_controll_handler))

	s := &http.Server{
		Addr:           ":9090",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Fatal(s.ListenAndServe())
}