import paramiko

def hive_query_executor():
    dns_name = '10.18.31.21'
    conn_obj = paramiko.SSHClient()
    conn_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        conn_obj.connect(dns_name, username="varun17201",password='Otsuka9$')

        Hive_query = "set hive.cli.print.header=true; select * from clinical_ink_ptsd_production.dc limit 5 "
        query_execute_command = 'hive -e "' + Hive_query + '"'
        stdin, stdout, stderr = conn_obj.exec_command(query_execute_command)

        output = stdout.read()
        print(output)

        conn_obj.close()

    except:
        print("Error :")
        exit(0)

def main():
    hive_query_executor()

if __name__ == "__main__": main()