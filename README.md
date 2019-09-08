# Mini-SQL-Engine

Basic sql engine which supports simple queries of the type:
```console
SELECT <attributes> FROM <tables> WHERE <condition>;
```

## How to run
* Run all the commands in one terminal tab
* Install python3 and python3-venv
```console
sudo apt install python3 python3-venv
```
* Create a virtual environment, activate the environment and install dependencies
```console
python3 -m venv env
source env/bin/activate
pip install --upgrade pip && pip install -r requirements.txt
```
* To run query, use the following command
```console
python3 20171171.py "<query_string>"
```

* After finishing running queries, deactivate environment
```console
deactivate
rm -r env/
```