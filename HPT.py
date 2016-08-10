#!/usr/bin/env python
from __future__ import ( division, absolute_import, print_function, unicode_literals )
import sys, os
import requests
import json
import ConfigFile
import urlparse
import gzip

valid_status = ['finished', 'delivered']


def handle_error(ex):
    if ex is not None and ex.message is not None:
        print("")
        print('{"Error": "' + str(ex.message) + '"}')
    elif ex is not None:
        print("")
        print('{"Error": "' + str(ex) + '"}')
    else:
        print("")
        print('{"Error": "(unknown error)"}')


def arg_count_check(minimum):
    if (len(sys.argv) - 2) < minimum:
        parameter_help()
        sys.exit(2)


def parameter_help():
    print(" ")
    print("Usage:")
    print("./HPT.py ACTION [ parameters ]")
    print("./HPT.py set-account-name ACCOUNT-NAME")
    print("./HPT.py set-username USERNAME")
    print("./HPT.py set-password PASSWORD")
    print("./HPT.py set-download-location DIRECTORY")
    print("./HPT.py create-job FILENAME.JSON")
    print("./HPT.py get-jobs [JOB_TYPE_FILTER(s) - " +
          "(O)pened, (E)stimating, (Q)uoted, (A)ccepted, (R)ejected, r(U)nning, (C)ompleted, (D)elivered, (F)ailed]")
    print("./HPT.py get-job-status JOB-ID")
    print("./HPT.py accept-job JOB-ID")
    print("./HPT.py reject-job JOB-ID")
    print("./HPT.py get-job-results JOB-ID [no-files] (removes urlList from results)")
    print("./HPT.py download-job JOB-ID [start-file-number] (optional # to resume stalled jobs)")
    print("./HPT.py download-from-results RESULTS.JSON [start-file-number] (optional # to resume stalled jobs)")
    print("./HPT.py validate-job JOB-ID    (confirms all files downloaded correctly and generates summary files)")


def get_response(method, endpoint, data=None):
    headers = {'Content-Type':'application/json'}
    auth_info = ConfigFile.get_settings("gnip.cfg", "basic")
    username = auth_info["username"]
    password = auth_info["password"]
    account = auth_info["account"]
    base_url = 'https://gnip-api.gnip.com/historical/powertrack/accounts/' + account + '/publishers/twitter/jobs'

    # print ("url:" + base_url + endpoint)
    response = None
    try:
        if method.lower() == "get":
            response = requests.request("GET",
                                        base_url + endpoint,
                                        headers=headers,
                                        auth=(username, password))
        elif method.lower() in ['post', 'put']:
            response = requests.request(method.upper(),
                                        base_url + endpoint,
                                        headers = headers,
                                        json=data,
                                        auth=(username, password))

        if response is not None and response.ok:
            if "json" in str(response.headers["Content-Type"]):
                return response.json()
            else:
                return response.text
        else:
            if response is not None:
                # print("status code:" + str(response.status_code))
                if response.status_code in [400, 404]:
                    return response.text
                else:
                    raise Exception("Error: in get_response " +
                                    str(response.status_code) + ": " +
                                    response.text + " url:" +
                                    base_url + endpoint)
            else:
                raise Exception("Error: in get_response " + base_url + endpoint)

    except Exception as ex:
        handle_error(ex)
    return None


def download_file(url, uuid, directory):
    try:
        scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
        file_name_start = path.find(uuid) + len(uuid) + 1
        file_name = path[file_name_start:]
        file_name = uuid + "_" + file_name.replace("/","_")
        print ("Downloading: " + file_name, end="")
        sys.stdout.flush()
        file_request = None
        max_retries = 10
        try_count = 0
        success = False
        # trap timeout errors and auto retry
        while try_count < max_retries and success is False:
            try:
                file_request = requests.get(url, timeout=30)
                success = True
            except requests.RequestException:
                try_count += 1
                print(" - Timeout. Retrying.", end="")
                sys.stdout.flush()
            except Exception as exp:
                print("Error in download_file: " + str(exp))
        #  didn't work...
        if try_count >= max_retries and success is False:
            raise Exception("Too many retries.  Failing.")
            return False

        output = open(directory + file_name, 'wb')
        output.write(file_request.content)
        output.close()
        print(" - done.")
        return True
    except Exception as e:
        handle_error(Exception("Error Downloading file " + str(e.message)))
        return False


def download_files(url_list, uuid, start_file=0):
    try:
        file_count = 0
        hpt_preferences = ConfigFile.get_settings("gnip.cfg", "HPT")
        download_status = True
        print("Starting at file:" + str(start_file))
        print("Downloading files to: " + hpt_preferences["destination"])
        for url in url_list:
            # abort if a file fails.
            if download_status:
                file_count += 1
                # skip if file is less than start file #
                if file_count >= int(start_file):
                    print ("#" + str(file_count), end=": ")
                    download_status = download_file(url, uuid, hpt_preferences["destination"])
            else:
                print("")
                return False
        print("")
        print("Done!")
        return True

    except Exception as e:
        print("")
        handle_error(e)
        return False


def download_from_results(file_name, start_file=0):
    try:
        results = json.loads(open(file_name, "r").read())
        if results is not None:
            print("Number of files:", results['urlCount'])
            if results['urlCount'] == 0:
                print("Nothing to do:.  " + results['urlCount'] + " files available to download.")
            else:
                uuid = results["urlList"][0][150:160]
                success = download_files(results['urlList'], uuid, start_file)
                if success:
                    return {"results": "Successfully downloaded files"}
                else:
                    return {"results": "Error downloading files"}
        else:
            print("Results file not found or empty")
            return
    except Exception as e:
        handle_error(e)
        return {"results": "Error downloading files"}


def download_job(uuid, start_file=0):
    try:
        print("Getting Status of job: " + uuid)
        job_info = get_job_status(uuid)
        if job_info['status'] in valid_status and job_info['percentComplete'] == 100:
            results = get_job_results(uuid)
            if results is not None:
                print("Number of files:", results['urlCount'])
                if results['urlCount'] == 0:
                    print("Nothing to do:.  " + results['urlCount'] + " files available to download.")
                else:
                    download_files(results['urlList'], uuid, start_file)
                    return {"result": "Successfully downloaded job"}
        else:
            print("Current job status: " + job_info["status"] + " - Complete: " + job_info["percentComplete"] + "%")
            return {"result": "Nothing to do"}
    except Exception as e:
        handle_error(e)
        return {"result": "Error downloading job"}


def set_account_name(account_name):
    try:
        ConfigFile.set_property("gnip.cfg", "basic", "account", account_name)
        return {"result": "success"}
    except Exception as ex:
        handle_error(ex)
        return {"result": "failed"}


def set_username(username):
    try:
        ConfigFile.set_property("gnip.cfg", "basic", "username", username)
        return {"result": "success"}
    except Exception as ex:
        handle_error(ex)
    return {"result": "failed"}


def set_password(password):
    try:
        ConfigFile.set_property("gnip.cfg", "basic", "password", password)
        return {"result": "success"}
    except Exception as ex:
        handle_error(ex)
    return {"result": "failed"}


def set_download_location(location):
    try:
        ConfigFile.set_property("gnip.cfg", "HPT", "destination", location)
        return {"result": "success"}
    except Exception as ex:
        handle_error(ex)
    return {"result": "failed"}


def create_job(file_name):
    try:
        with open(file_name, 'rb') as f:
            data = f.read()  # produces single string
            if data is not None:
                job_data = json.loads(data)
                create_response = get_response('post', '.json', job_data)
                return create_response
            else:
                Exception("Error: " + file_name + " is empty or not found.")
    except Exception as ex:
        handle_error(ex)
        return None


def get_jobs():
    jobs_status = get_response('get', '.json')
    if jobs_status is not None:
        return jobs_status
    return None


def get_job_status(uuid):
    job_status = get_response('get', "/" + uuid + ".json")
    return job_status


def get_job_results(uuid, no_files_flag=False):
    job_results = get_response('get', "/" + uuid + "/results.json")
    if no_files_flag:
        del job_results["urlList"]
    return job_results


def accept_job(uuid):
    data = {"status": "accept"}
    job_status = get_response('put', "/" + uuid + ".json", data)
    return job_status


def reject_job(uuid):
    data = {"status": "reject"}
    job_status = get_response('put', "/" + uuid + ".json", data)
    return job_status


def validate_file(url, uuid, directory):
    file_name = "{None}"
    try:
        scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
        file_name_start = path.find(uuid) + len(uuid) + 1
        file_name = path[file_name_start:]
        file_name = uuid + "_" + file_name.replace("/","_")
        # print ("Validating: " + file_name, end="")
        sys.stdout.flush()

        # validate file exists
        if not os.path.isfile(directory + file_name):
            raise OSError("File " + file_name + " not found")

        with gzip.open(directory + file_name, 'rb') as f:
            file_content = f.read()
            lines = file_content.splitlines()
            last_line = lines[-1]
            status_line = json.loads(last_line)
            if "info" in status_line:
                # print("- good!")
                status_line["file"] = file_name
                return status_line
            else:
                # print("- error.")
                return {"error": "Status line not found in " + file_name}

    except Exception as e:
        handle_error(e)
        return {"error": str(e)}


def validate_files(url_list, uuid):
    try:
        file_count = 0
        valid_info = []
        error_status = []
        hpt_preferences = ConfigFile.get_settings("gnip.cfg", "HPT")
        validate_status = True
        print("Validating files in: " + hpt_preferences["destination"])
        activities_count = 0
        for url in url_list:
            file_count += 1
            # print ("#" + str(file_count), end=": ")
            print(".", end="")
            validate_status = validate_file(url, uuid, hpt_preferences["destination"])
            if "info" in validate_status:
                valid_info.append(validate_status)
                activities_count += validate_status["info"]["activity_count"]
            else:
                error_status.append(validate_status)
        print("")
        print("Done!")

        hpt_preferences = ConfigFile.get_settings("gnip.cfg", "HPT")
        directory  = hpt_preferences["destination"]
        output = open(directory + uuid + "-validation.json", 'wb')
        output.write(json.dumps({"results": valid_info}))
        output.close()

        if len(error_status) > 0:
            output = open(directory + uuid + "-errors.json", 'wb')
            output.write(json.dumps({"results": error_status}))
            output.close()
        else:
            if os.path.isfile(directory + uuid + "-errors.json"):
                os.remove(directory + uuid + "-errors.json")

        return {"results": {"activities": activities_count, "errors": error_status}}

    except Exception as e:
        print("")
        handle_error(e)
        return {"results": {"errors": "Abnormally ended", "Message": str(e)}}


def validate_job(uuid):
    try:
        print("Getting Status of job: " + uuid)
        job_info = get_job_status(uuid)
        if job_info['status'] in valid_status and job_info['percentComplete'] == 100:
            results = get_job_results(uuid)
            if results is not None:
                print("Number of files:", results['urlCount'])
                if results['urlCount'] == 0:
                    print("Nothing to do:.  " + results['urlCount'] + " files available to validate.")
                else:
                    return validate_files(results['urlList'], uuid)
        else:
            print("Current job status: " + job_info["status"] + " - Complete: " + job_info["percentComplete"] + "%")
            return {"result": "Nothing to do"}
    except Exception as e:
        handle_error(e)
        return {"result": "Error validating job"}
    return {"result": "Successfully validated job"}


if __name__ == "__main__":
    # 0 arguments means that a command was passed, but no arguments, valid for "get-jobs"
    arg_count_check(0)
    action = sys.argv[1]

    result = None

    if action.lower() == "set-account-name":
        arg_count_check(1)
        result = set_account_name(sys.argv[2])
    elif action.lower() == "set-username":
        arg_count_check(1)
        result = set_username(sys.argv[2])
    elif action.lower() == "set-password":
        arg_count_check(1)
        result = set_password(sys.argv[2])
    elif action.lower() == "set-download-location":
        arg_count_check(1)
        result = set_download_location(sys.argv[2])
    elif action.lower() == "create-job":
        arg_count_check(1)
        result = create_job(sys.argv[2])
    elif action.lower() == "get-jobs":
        arg_count_check(0)
        result = get_jobs()
    elif action.lower() == "get-job-status":
        arg_count_check(1)
        result = get_job_status(sys.argv[2])
    elif action.lower() == "get-job-results":
        arg_count_check(1)
        no_files = len(sys.argv) > 3 and str(sys.argv[3]).lower() == "no-files"
        result = get_job_results(sys.argv[2], no_files)
    elif action.lower() == "accept-job":
        arg_count_check(1)
        result = accept_job(sys.argv[2])
    elif action.lower() == "reject-job":
        arg_count_check(1)
        result = reject_job(sys.argv[2])
    elif action.lower() == "download-job":
        arg_count_check(1)
        start = 0
        if len(sys.argv) > 3:
            start = sys.argv[3]
        result = download_job(sys.argv[2], start)
    elif action.lower() == "download-from-results":
        arg_count_check(1)
        start = 0
        if len(sys.argv) > 3:
            start = int(sys.argv[3])
        result = download_from_results(sys.argv[2], start)
    elif action.lower() == "validate-job":
        arg_count_check(1)
        result = validate_job(sys.argv[2])
    else:
        parameter_help()

    if result is not None:
        print(json.dumps(result, sort_keys=True, indent=2))
