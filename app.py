from flask import Flask
from flask import render_template
from flask import request
import boto3
from pymysql import connections
import urllib.request
from config import *
app = Flask(__name__)
db_conn = connections.Connection(
    host=databasehost,
    port=3306,
    user=duser,
    password=dpass,
    db=s3database

)


@app.route("/", methods=['GET', 'POST'])
def hello_world():
    print('inside function')
    if request.method == 'POST':
        print('inside if')
        print('inside if repeats')
        session = boto3.Session(region_name='us-east-1', aws_access_key_id='AKIASBGQBM76RSBPWIHE', aws_secret_access_key='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')
        s3 = session.resource('s3')
        file_body = request.files['file_name']
        count_obj = 0
        for i in s3.Bucket(custombucket).objects.all():
            count_obj = count_obj + 1
        file_name = "file-id-" + str(count_obj + 1)
        print('before try')
        try:
            s3.Bucket(custombucket).put_object(Key=file_name, Body=file_body)
            s3_location = 'us-east-1'
            if s3_location is None:
                s3_location = ''
            else:
                s3_location = '-' + s3_location
            print('s3locatopm',s3_location)
            object_url = "https://s3{0}.amazonaws.com/{1}/{2}".format(
                s3_location,
                custombucket,
                file_name)
            print("objurl",object_url)
            try:
                cursor = db_conn.cursor()
                insert_sql = "INSERT INTO intellipaat VALUES (%s, %s)"
                cursor.execute(insert_sql, (file_name, object_url))
                db_conn.commit()

            except Exception as e:
                return str(e)

        except Exception as e:
            return str(e)
    print("Uploading to S3 success... ")
    return render_template("index.html")


@app.route("/check", methods=['GET', 'POST'])
def hello():
    try:
        statuscode = urllib.request.urlopen(kapp).getcode()
        if statuscode == 200:
            return "<h1>Cluster is up!</h1>"
    except Exception as e:
        return "<h1>Cluster is not up!</h1>"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
