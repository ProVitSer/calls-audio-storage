package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"
	"strings"
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
	CallLegsAPrefix = "-in"
	CallLegsBPrefix = "-out"
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
    Uniqueid 	string `json:"uniqueid"`
}

type RecordingsInfo struct {
    S3LegsPath   string
    RecordName 	 string
}


func checkErr(err error) {
	if err != nil {
		log.Println(err)
	}
}

func set_call_controll_info(uniqueid string){

	time.Sleep(30 * time.Second)

	call_info := get_call_info(uniqueid)

	record_name := strings.TrimSuffix(call_info.Recordingfile, ".wav")

	records_info := get_recordings_info(record_name)

	save_and_del_record(records_info)

}

func save_and_del_record(records_info []RecordingsInfo){
	for _, obj := range records_info {
		save_recoding_to_s3(get_recording_path(obj.RecordName), obj.S3LegsPath)
		del_recording_file(get_recording_path(obj.RecordName))
    }
}

func del_recording_file(recording_path string){
	err := os.Remove(recording_path)
    if err != nil {
        log.Println("Ошибка удаления файла:", err)
        return
    }

    log.Println("Файл успешно удален")
}

func get_recordings_info(record_name string)([]RecordingsInfo){
	recordObjects := []RecordingsInfo{
        {S3LegsPath: CallClientPath, RecordName: fmt.Sprintf("%s%s.wav", record_name, CallLegsAPrefix)},
        {S3LegsPath: CallCompanyPath, RecordName: fmt.Sprintf("%s%s.wav", record_name, CallLegsBPrefix)},
    }

	return recordObjects
}

func save_recoding_to_s3(localRecordFilePath string, s3FolderPath string){

    cmd := exec.Command("s3cmd", "put", localRecordFilePath, fmt.Sprintf("s3://%s/%s/%s", BucketName, s3FolderPath, get_date_path()))

    output, err := cmd.CombinedOutput()
    if err != nil {
        log.Println("Ошибка при выполнении команды s3cmd:", err)
        return
    }

    log.Println("Результат выполнения команды s3cmd:")
    log.Println(string(output))
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
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)

    if err != nil {
        log.Fatal("Ошибка открытия файла логов:", err)
    }

    defer logFile.Close()

    log.SetOutput(logFile)

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