import os
import paramiko
import time

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
ssh.connect('10.18.31.21', username='varun17201', password='Otsuka9$')
sshin, sshout, ssherr= ssh.exec_command('hive -e "set hive.cli.print.header=true; select * from clinical_ink_ptsd_production.dc limit 5"')
output = sshout.read()
print(output)