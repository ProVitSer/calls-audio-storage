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
    _ "encoding/json"
    "path/filepath"
    _ "github.com/go-sql-driver/mysql"
    _ "github.com/lib/pq"
    _ "io/ioutil"
    "github.com/joho/godotenv"
    "strconv"
)

const (
    RecordBaseDir = "/var/spool/asterisk/monitor"
    BucketName = "app-sales-reports-calls-audio-storage"
    CallCompanyPath = "audio/call-company"
    CallClientPath = "audio/call-client"
    CallAllPath = "audio/call-all"
    CallLegsAPrefix = "-in"
    CallLegsBPrefix = "-out"
)

var (
    err error 
	mysqldb * sql.DB 
	pgdb * sql.DB 
	call CallInfo
)

type CallInfo struct {
    Uniqueid string `json:"uniqueid"`
    Linkedid string `json:"linkedid"`
    Disposition string `json:"disposition"`
    Duration string `json:"duration"`
    Billsec string `json:"billsec"`
    Channel string `json:"channel"`
    Dstchannel string `json:"dstchannel"`
    Lastapp string `json:"lastapp"`
    Lastdata string `json:"lastdata"`
    Recordingfile string `json:"recordingfile"`
}

type handler func(w http.ResponseWriter, r * http.Request)

type RequestData struct {
    Uniqueid string `json:"uniqueid"`
}

type RecordingsInfo struct {
    CallTypes string
    S3FolderPath string
    RecordName string
}


func checkErr(err error) {
    if err != nil {
        log.Println(err)
    }
}

func set_call_controll_info(uniqueid string) {

    time.Sleep(5 * time.Second)

    call_info: = get_call_info(uniqueid)
    record_name: = strings.TrimSuffix(call_info.Recordingfile, ".wav")
    records_info: = get_recordings_info(record_name)
    save_records(records_info)
    save_call_info(call_info, records_info)

}

func save_call_info(call_info CallInfo, records_info[] RecordingsInfo) {
    call_duration_dial: = get_call_duration_dial(call_info)
    call_timestamp: = get_current_time()

        recordsMap: = make(map[string] RecordingsInfo)

    for _,
    record: = range records_info {
        recordsMap[record.CallTypes] = record
    }

    call_manager_speech_url: = fmt.Sprintf("%s%s", recordsMap["legsB"].S3FolderPath, recordsMap["legsB"].RecordName)
    call_client_speech_url: = fmt.Sprintf("%s%s", recordsMap["legsA"].S3FolderPath, recordsMap["legsA"].RecordName)
    call_all_speech_url: = fmt.Sprintf("%s%s", recordsMap["all"].S3FolderPath, recordsMap["all"].RecordName)


    log.Println(call_info.Uniqueid, call_info.Linkedid, call_timestamp, call_info.Disposition, call_duration_dial, call_info.Billsec, call_info.Duration, call_info.Channel, call_info.Dstchannel, call_manager_speech_url, call_client_speech_url, call_all_speech_url)

    _, err: = pgdb.Exec("INSERT INTO app_ate_processing_v1.call_insertable (call_unique_id,call_linked_id,call_timestamp,call_disposition_id,call_duration_dial,call_duration_talk,call_duration_total,call_channel_src,call_channel_dst,call_manager_speech_url,call_client_speech_url,call_all_speech_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", call_info.Uniqueid, call_info.Linkedid, call_timestamp, call_info.Disposition, call_duration_dial, call_info.Billsec, call_info.Duration, call_info.Channel, call_info.Dstchannel, call_manager_speech_url, call_client_speech_url, call_all_speech_url)

    checkErr(err)
}

func get_call_duration_dial(call_info CallInfo) string {
    duration,
    err1: = strconv.Atoi(call_info.Duration)
    if err1 != nil {
        panic(err1)
    }

    billsec,
    err2: = strconv.Atoi(call_info.Billsec)
    if err2 != nil {
        panic(err2)
    }

    result: = duration - billsec
    return strconv.Itoa(result)
}

func save_records(records_info[] RecordingsInfo) {
    for _, obj: = range records_info {
        save_recoding_to_s3(get_recording_path(obj.RecordName), obj.S3FolderPath)
    }
}


func get_recordings_info(record_name string)([] RecordingsInfo) {
    recordObjects: = [] RecordingsInfo {
        {
            CallTypes: "legsA",
            S3FolderPath: fmt.Sprintf("s3://%s/%s/%s", BucketName, CallClientPath, get_date_path()),
            RecordName: fmt.Sprintf("%s%s.wav", record_name, CallLegsAPrefix)
        }, {
            CallTypes: "legsB",
            S3FolderPath: fmt.Sprintf("s3://%s/%s/%s", BucketName, CallCompanyPath, get_date_path()),
            RecordName: fmt.Sprintf("%s%s.wav", record_name, CallLegsBPrefix)
        }, {
            CallTypes: "all",
            S3FolderPath: fmt.Sprintf("s3://%s/%s/%s", BucketName, CallAllPath, get_date_path()),
            RecordName: fmt.Sprintf("%s.wav", record_name)
        },
    }

        return recordObjects
}

func save_recoding_to_s3(localRecordFilePath string, s3FolderPath string) {

    cmd: = exec.Command("s3cmd", "put", localRecordFilePath, s3FolderPath)

    output,err: = cmd.CombinedOutput()
    if err != nil {
        log.Fatalf("Ошибка при выполнении команды s3cmd:", err)
        return
    }

    log.Println("Результат выполнения команды s3cmd:")
    log.Println(string(output))
}


func get_call_info(uniqueid string)(CallInfo) {

    var call_info CallInfo

    call_data, err: = mysqldb.Query("select uniqueid, linkedid, disposition, duration, billsec, channel, dstchannel, lastapp, lastdata, recordingfile from cdr where uniqueid like ?", uniqueid)

    checkErr(err)

    defer call_data.Close()

    if call_data.Next() {
        err: = call_data.Scan( & call_info.Uniqueid, & call_info.Linkedid, & call_info.Disposition, & call_info.Duration, & call_info.Billsec, & call_info.Channel, & call_info.Dstchannel, & call_info.Lastapp, & call_info.Lastdata, & call_info.Recordingfile, )
        checkErr(err)
    }

    return call_info
}

func get_recording_path(recordingfile string) string {
    filePath: = filepath.Join(RecordBaseDir, get_date_path(), recordingfile)
    return filePath
}

func get_date_path() string {
    now: = time.Now()
    datePath: = fmt.Sprintf("%d/%02d/%02d/", now.Year(), now.Month(), now.Day())
    return datePath
}

func get_current_time() time.Time {
    utcTime: = time.Now().UTC()
    location: = time.FixedZone("UTC+3", 3 * 60 * 60)
    timeInUTCPlus3: = utcTime.In(location)
    return timeInUTCPlus3
}

func PostOnly(h handler) handler {
    return func(w http.ResponseWriter, r * http.Request) {
        if r.Method == "POST" {
            h(w, r)
            return
        }
    }
}

func call_controll_handler(w http.ResponseWriter, r * http.Request) {
    r.ParseForm()

    uniqueid: = r.FormValue("uniqueid")

    w.WriteHeader(http.StatusOK)

    go func(uniqueid string) {
        set_call_controll_info(uniqueid)
    }(uniqueid)

    return
}

func goDotEnvVariable(key string) string {

    err: = godotenv.Load(".env")

    if err != nil {
        log.Fatalf("Error loading .env file")
    }

    return os.Getenv(key)
}


func main() {
    logFile,
    err: = os.OpenFile("app.log", os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0666)

    if err != nil {
        log.Fatal("Ошибка открытия файла логов:", err)
    }

    defer logFile.Close()

    log.SetOutput(logFile)

    connectionString: = fmt.Sprintf("%s:%s@(%s:%s)/%s", goDotEnvVariable("MYSQL_USERNAME"), goDotEnvVariable("MYSQL_PASSWORD"), "127.0.0.1", "3306", "asteriskcdrdb")

    mysqldb, _ = sql.Open("mysql", connectionString)

    err = mysqldb.Ping()

    checkErr(err)

    defer mysqldb.Close()


    connectString: = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", goDotEnvVariable("PG_USERNAME"), goDotEnvVariable("PG_PASSWORD"), goDotEnvVariable("PG_HOST"), goDotEnvVariable("PG_PORT"), goDotEnvVariable("PG_DB"))

    pgdb, err = sql.Open("postgres", connectString)

    checkErr(err)

    defer pgdb.Close()


    http.HandleFunc("/call-controll", PostOnly(call_controll_handler))

    s: = & http.Server {
        Addr: ":9090",
        ReadTimeout: 10 * time.Second,
        WriteTimeout: 10 * time.Second,
        MaxHeaderBytes: 1 << 20,
    }

    log.Fatal(s.ListenAndServe())
}