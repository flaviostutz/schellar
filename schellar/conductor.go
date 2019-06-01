package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
)

func launchWorkflow(scheduleID string) (string, error) {
	logrus.Debugf("startWorkflow scheduleId=%s", scheduleID)

	logrus.Debugf("Loading schedule definitions from DB")
	var schedule Schedule
	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	err := st.FindId(scheduleID).One(&schedule)
	if err != nil {
		logrus.Errorf("Couldn't find schedule %s", scheduleID)
		return "", err
	}

	wf := make(map[string]interface{})
	wf["name"] = schedule.WorkflowName
	wf["version"] = schedule.WorkflowVersion
	wf["input"] = schedule.CurrentContext
	// wf["taskToDomain"] = schedule.WorkflowTaskToDomain
	// wf["correlationId"] = schedule.WorkflowCorrelationID
	wfb, _ := json.Marshal(wf)

	logrus.Debugf("Launching Workflow %s", wf)
	url := fmt.Sprintf("%/workflow", conductorURL)
	resp, data, err := postHTTP(url, wfb)
	if err != nil {
		logrus.Errorf("Call to Conductor POST /workflow failed. err=%s", err)
		return "", err
	}
	if resp.StatusCode != 200 {
		logrus.Warnf("POST /workflow call status != 200. resp=%s", resp)
		return "", fmt.Errorf("Failed to create new workflow instance. status=%s", resp.StatusCode)
	}
	logrus.Infof("Workflow launched successfully. workflowName=%s workflowId=%s", schedule.WorkflowName, string(data))
	return string(data), nil
}

func getWorkflow(name string, version string) (map[string]interface{}, error) {
	logrus.Debugf("getWorkflow %s", name)
	resp, data, err := getHTTP(fmt.Sprintf("%s/metadata/workflow/{%s}?version=%s", name, version))
	if err != nil {
		return nil, fmt.Errorf("GET /metadata/workflow/name failed. err=%s", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Couldn't get workflow info. name=%s", name)
	}
	var wfdata map[string]interface{}
	err = json.Unmarshal(data, &wfdata)
	if err != nil {
		logrus.Errorf("Error parsing json. err=%s", err)
		return nil, err
	}
	return wfdata, nil
}

func getWorkflowRun(workflowID string) (map[string]interface{}, error) {
	logrus.Debugf("getWorkflowRun %s", workflowID)
	resp, data, err := getHTTP(fmt.Sprintf("%s/workflow/%s?includeTasks=false", conductorURL, workflowID))
	if err != nil {
		logrus.Errorf("GET /workflow/id invocation failed. err=%s", err)
		return nil, fmt.Errorf("GET /workflow/id failed. err=%s", err)
	}
	if resp.StatusCode != 200 {
		logrus.Warnf("GET /workflow/%s status!=200 resp=%s", workflowID, resp)
		return nil, fmt.Errorf("Couldn't get workflow info. id=%s", workflowID)
	}
	var wfdata map[string]interface{}
	err = json.Unmarshal(data, &wfdata)
	if err != nil {
		logrus.Errorf("Error parsing json. err=%s", err)
		return nil, err
	}
	return wfdata, nil
}

func postHTTP(url string, data []byte) (http.Response, []byte, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		logrus.Errorf("HTTP request creation failed. err=%s", err)
		return http.Response{}, []byte{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	logrus.Debugf("POST request=%s", req)
	response, err1 := client.Do(req)
	if err1 != nil {
		logrus.Errorf("HTTP request invocation failed. err=%s", err1)
		return http.Response{}, []byte{}, err1
	}

	logrus.Debugf("Response: %s", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)
	return *response, datar, nil
}

func getHTTP(url string) (http.Response, []byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logrus.Errorf("HTTP request creation failed. err=%s", err)
		return http.Response{}, []byte{}, err
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	logrus.Debugf("GET request=%s", req)
	response, err1 := client.Do(req)
	if err1 != nil {
		logrus.Errorf("HTTP request invocation failed. err=%s", err1)
		return http.Response{}, []byte{}, err1
	}

	logrus.Debugf("Response: %s", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)
	return *response, datar, nil
}
