import subprocess

def lambda_handler(event, context):
    my_output = subprocess.check_output(["curl","-X", "GET", "https://www.httpbin.org/get"], stderr=subprocess.STDOUT, shell=False)
    print(my_output.decode('utf8'))