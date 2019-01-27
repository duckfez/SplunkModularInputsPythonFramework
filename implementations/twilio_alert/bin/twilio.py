import sys,os,hashlib
import json
  

SPLUNK_HOME = os.environ.get("SPLUNK_HOME")
#dynamically load in any eggs in /etc/apps/snmp_ta/bin 
EGG_DIR = SPLUNK_HOME + "/etc/apps/twilio_alert/bin/"

for filename in os.listdir(EGG_DIR):
    if filename.endswith(".egg"):
        sys.path.append(EGG_DIR + filename)

from twilio.rest import TwilioRestClient

def send_message(settings):
    print >> sys.stderr, "DEBUG Sending message with settings %s" % settings
    
    account_sid = settings.get('accountsid')
    auth_token = settings.get('authtoken')
    from_number = settings.get('fromnumber')
    to_number = settings.get('tonumber')
    message = settings.get('message')
    
    activation_key = settings.get('activationkey')
    app_name = "Twilio SMS Alerting"
    
    m = hashlib.md5()
    m.update((app_name))
    if not m.hexdigest().upper() == activation_key.upper():
        print >> sys.stderr, ("FATAL Activation key for App '%s' failed" % app_name)
        sys.exit(2)
  
    try:  
        client = TwilioRestClient(account_sid, auth_token)  
  
        numbers_list = to_number.split(",") 
        
        for number in numbers_list:  
            print >> sys.stderr, "INFO Sending SMS via Twilio from number=%s to number=%s with message=%s" % (from_number, number,message)   
            message_resp = client.messages.create(body=message,to=number,from_=from_number)    
            print >> sys.stderr, "INFO Sent Twilio SMS message with sid=%s" % message_resp.sid
          
        return True  
    except Exception as tre:  
        print >> sys.stderr,tre  
        return False  
    except:  
        e = sys.exc_info()[0]  
        print >> sys.stderr, "ERROR Error sending SMS message via Twilio: %s" % e  
        return False  
  
  
if __name__ == "__main__":  
    if len(sys.argv) > 1 and sys.argv[1] == "--execute":  
        payload = json.loads(sys.stdin.read())
        if not send_message(payload.get('configuration')):
            print >> sys.stderr, "FATAL Failed trying to send SMS Message via Twilio"
            sys.exit(2)
        else:
            print >> sys.stderr, "INFO SMS Message successfully sent via Twilio"
    else:
        print >> sys.stderr, "FATAL Unsupported execution mode (expected --execute flag)"
        sys.exit(1)
