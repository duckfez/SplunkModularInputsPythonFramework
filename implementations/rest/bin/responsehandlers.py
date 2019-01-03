#add your custom response handler class to this module
import json
import datetime
import sys
import re
from datetime import datetime,timedelta

#the default handler , does nothing , just passes the raw output directly to STDOUT
class DefaultResponseHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        cookies = response_object.cookies
        if cookies:
            req_args["cookies"] = cookies        
        print_xml_stream(raw_response_output,handle)
          
#template
class MyResponseHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        print_xml_stream("foobar",handle)
        

class RollOutCSVHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        import csv,io
        reader_list = csv.DictReader(io.StringIO(raw_response_output))
        for row in reader_list:      
            print_xml_stream(row,handle)
        

'''various example handlers follow'''
        
class BoxEventHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":        
            output = json.loads(raw_response_output)
            if not "params" in req_args:
                req_args["params"] = {}
            if "next_stream_position" in output:    
                req_args["params"]["stream_position"] = output["next_stream_position"]
            for entry in output["entries"]:
                print_xml_stream(json.dumps(entry),handle)
        else:
            print_xml_stream(raw_response_output,handle)

class QualysGuardActivityLog:
    '''Response handler for QualysGuard activity log.'''

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if not "params" in req_args:
            req_args["params"] = {}
        date_from = (datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        req_args["params"]["date_from"] = date_from
        print_xml_stream(raw_response_output,handle)

class ZipFileResponseHandler:

    def __init__(self,**args):
        self.csv_file_to_index = args['csv_file_to_index']

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        import zipfile,io,re
        file = zipfile.ZipFile(BytesIO(response_object.content))
        for info in file.infolist():
            if re.match(self.csv_file_to_index, info.filename):
                filecontent = file.read(info)
                print_xml_stream(filecontent,handle)
      

class FourSquareCheckinsEventHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":        
            output = json.loads(raw_response_output)
            last_created_at = 0
            for checkin in output["response"]["checkins"]["items"]:
                print_xml_stream(json.dumps(checkin),handle)
                if "createdAt" in checkin:
                    created_at = checkin["createdAt"]
                    if created_at > last_created_at:
                        last_created_at = created_at
            if not "params" in req_args:
                req_args["params"] = {}
            
            req_args["params"]["afterTimestamp"] = last_created_at
                      
        else:
            print_xml_stream(raw_response_output,handle)
            
class ThingWorxTagHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":        
            output = json.loads(raw_response_output)
            for row in output["rows"]:
                print_xml_stream(json.dumps(row),handle)
        else:
            print_xml_stream(raw_response_output,handle)
            
class FireEyeEventHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":        
            output = json.loads(response_object.content)
            last_display_id = -1
            for alert in output["alerts"]:
                print_xml_stream(json.dumps(alert),handle)
                if "displayId" in alert:
                    display_id = alert["displayId"]
                    if display_id > last_display_id:
                        last_display_id = display_id
            if not "params" in req_args:
                req_args["params"] = {}
            
            if last_display_id > -1:
                req_args["params"]["offset"] = last_display_id

        else:
            print_xml_stream(raw_response_output,handle)
              
        

class CallIdentifierHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":        
            output = json.loads(raw_response_output,handle)
            
            for call in output["plcmCallList"]:
                del call["atomLinkList"]
                del call["destinationDetails"]
                del call["originatorDetails"]
                print_xml_stream(json.dumps(call),handle)
        else:
            print_xml_stream(raw_response_output,handle)

class ExampleHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":        
            output = json.loads(raw_response_output)
            
            for item in output["data"]:
                print_xml_stream(json.dumps(item),handle)
        else:
            print_xml_stream(raw_response_output,handle)

class MyCustomHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        
        req_args["data"] = 'What does the fox say'   
         
        print_xml_stream(raw_response_output,handle)
                               

class TwitterEventHandler:

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
            
        if response_type == "json":        
            output = json.loads(raw_response_output)
            last_tweet_indexed_id = 0
            for twitter_event in output:
                print_xml_stream(json.dumps(twitter_event),handle)
                if "id_str" in twitter_event:
                    tweet_id = twitter_event["id_str"]
                    if tweet_id > last_tweet_indexed_id:
                        last_tweet_indexed_id = tweet_id
            
            if not "params" in req_args:
                req_args["params"] = {}
            
            req_args["params"]["since_id"] = last_tweet_indexed_id
                       
        else:
            print_xml_stream(raw_response_output,handle)
     

                
class AutomaticEventHandler:

    def __init__(self,**args):
        pass

    #process the received JSON array     
    def process_automatic_response(data,handle):
    
        output = json.loads(data)
        last_end_time = 0
                    
        for event in output:
            #each element of the array is written to Splunk as a seperate event
            print_xml_stream(json.dumps(event),handle)
            if "end_time" in event:
                #get and set the latest end_time
                end_time = event["end_time"]
                if end_time > last_end_time:
                    last_end_time = end_time
        return last_end_time

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
            
        if response_type == "json":
            last_end_time = 0
            
            #process the response from the orginal request
            end_time = process_automatic_response(raw_response_output,handle)
            
            #set the latest end_time
            if end_time > last_end_time:
                last_end_time = end_time
             
            #follow any pagination links in the response    
            next_link = response_object.links["next"] 
                   
            while next_link:
                next_response = requests.get(next_link)       
                end_time = process_automatic_response(next_response.text)  
                #set the latest end_time 
                if end_time > last_end_time:
                    last_end_time = end_time  
                next_link = next_response.links["next"]
                        
            if not "params" in req_args:
                req_args["params"] = {}
            
            #set the start URL attribute for the next request
            #the Mod Input will persist this to inputs.conf for you
            req_args["params"]["start"] = last_end_time
                       
        else:
            print_xml_stream(raw_response_output,handle)
            
class AirTableEventHandler2:
 
     def __init__(self,**args):
         pass
 
     def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
         if response_type == "json":
             output = json.loads(raw_response_output)
             
             #first response
             for record in output["records"]:
                 print_xml_stream(json.dumps(record),handle)
            
             offset = output["offset"]   
             #pagination loop    
             while offset is not None:
                 
                 next_url = response_object.url+'?offset='+offset
                 next_response = requests.get(next_url)
                 output = json.loads(next_response.text)
                 #print out results from pagination looping
                 for record in output["records"]:
                     print_xml_stream(json.dumps(record),handle)
                 #hopefully (guessing) at the end of the pagination , there will be
                 #no more "offset" values in the JSON response , so this will cause the while
                 #loop to exit   
                 if "offset" in output:
                     offset = output["offset"]
                 else:
                     offset = None 
                 
                 
 
         else:
             print_xml_stream(raw_response_output,handle)
                        
            
class OpenstackTelemetryHandler:

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
            
        if response_type == "json":        
            output = json.loads(raw_response_output)
            timestamp = 0
            for counter in output:
                print_xml_stream(json.dumps(counter),handle)
                if "timestamp" in counter:
                    temp_timestamp = counter["timestamp"]
                    if temp_timestamp > timestamp:
                        timestamp = temp_timestamp
            
            if not "params" in req_args:
                req_args["params"] = {}
            
            req_args["params"]["q.value"] = timestamp
                       
        else:
            print_xml_stream(raw_response_output,handle)

class SmartTabHandler:

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":
            output = json.loads(raw_response_output)

            #split out JSON array elements into individual events
            for entry in output:
                print_xml_stream(json.dumps(entry),handle)
            
            if not "params" in req_args:
                req_args["params"] = {}
            
            #increment the date parameters by 1 day. These will get automagically persisted
            #back to inputs.conf for you
            req_args["params"]["dateStart"] = increment_one_day(req_args["params"]["dateStart"])
            req_args["params"]["dateEnd"] = increment_one_day(req_args["params"]["dateEnd"])
            
        else:
            print_xml_stream(raw_response_output,handle)
            
    def _increment_one_day(self,date_str):

        date = datetime.strptime(date_str,'%Y-%m-%d')
        date += timedelta(days=1)
        return datetime.strftime(date,'%Y-%m-%d')

class HPEResponseHandler:

    def __init__(self,**args):
        #self.date_format = args['date_format']
        self.date_format = "%Y-%m-%dT%H:%M:%S.%f"
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        from datetime import datetime
        
        if response_type == "json":
            output = json.loads(raw_response_output)
            new_watermark = None
            #split out each event and keep track of latest update time for new watermark value
            for event in output["event_list"]["event"]:
                time_changed =  datetime.strptime(event["time_changed"][:23], self.date_format)
                if new_watermark is None or time_changed > new_watermark:
                    new_watermark = time_changed
                print_xml_stream(json.dumps(event),handle)
                
            if not "params" in req_args:
                req_args["params"] = {}
            
            #set watermark value for next request
            req_args["params"]["watermark"] = datetime.strftime(new_watermark,self.date_format)
            
        else:
            print_xml_stream(raw_response_output,handle)
                       
class JSONArrayHandler:

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":
            output = json.loads(raw_response_output)

            for entry in output:
                print_xml_stream(json.dumps(entry),handle)
        else:
            print_xml_stream(raw_response_output,handle)
            
class MyJSONArrayHandler:

    def __init__(self,**args):
        self.somekey = args['somekey']
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":
            output = json.loads(raw_response_output)

            for entry in output['value']:
                entry['somekey'] = self.somekey
                print_xml_stream(json.dumps(entry),handle)
        else:
            print_xml_stream(raw_response_output,handle)

class JoesResponseHandler:

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":
            output = json.loads(raw_response_output)
            last_id = 0
            for entry in output['entries']:
                print_xml_stream(json.dumps(entry),handle)
                if "EntryId" in entry:
                    this_id = entry["EntryId"]
                    if this_id > last_id:
                        last_id = this_id
            
            if not "params" in req_args:
                req_args["params"] = {}
            
            req_args["params"]["pageStart"] = last_id
        else:
            print_xml_stream(raw_response_output,handle)
            
class YourJSONArrayHandler:

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":
            raw_json = json.loads(raw_response_output)
            column_list = []
            for column in raw_json['columns']:
                column_list.append(column['name'])
            for row in raw_json['rows']:
                i = 0;
                new_event = {}
                for row_item in row:          
                    new_event[column_list[i]] = row_item
                    i = i+1
                print print_xml_stream(json.dumps(new_event),handle)

        else:
            print_xml_stream(raw_response_output,handle)       
                                      
            
    
class FlightInfoEventHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "json":        
            output = json.loads(raw_response_output)
            for flight in output["FlightInfoResult"]["flights"]:
                print_xml_stream(json.dumps(flight),handle) 
                
                      
        else:
            print_xml_stream(raw_response_output,handle) 
            
class AlarmHandler:
    
    def __init__(self,**args):
        pass
        
    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):
        if response_type == "xml": 
            import xml.etree.ElementTree as ET
            alarm_list = ET.fromstring(encodeXMLText(raw_response_output))
            for alarm in alarm_list:
                alarm_xml_str = ET.tostring(alarm, encoding='utf8', method='xml')
                print_xml_stream(alarm_xml_str,handle)
                      
        else:
            print_xml_stream(raw_response_output,handle)
                                                                                         
class WindowsDefenderATPHandler:

    # Bad hack follows.  Microsoft's API returns subseconds at a higher precision
    # than python's datetime will allow.  So, I'm padding the subseconds in order
    # to be able to do a string-comparison of timestamps (which is possible because
    # they're in ISO-8601 form

    def pad(t):
        rexp = "^(?P<date>[-0-9T:]+)(?:\.(?P<subseconds>\d+))?(?P<zone>.*)?$"
        x=re.match(rexp,t)
        if x is None:
            return t
        y=x.groupdict()

        if y.get('subseconds') is None:
            y['subseconds'] = ""

        if y.get('zone') in [ None, "" ]:
            y['zone'] = "Z"

        y['subseconds'] = (y['subseconds'] + '0000000000000')[0:7]

        return "".join([
            y['date'],
            '.',
            y['subseconds'],
            y['zone']
        ])

    def __init__(self,**args):
        pass

    def __call__(self, response_object,raw_response_output,response_type,req_args,endpoint,handle=sys.stdout):

        if not "params" in req_args:
            req_args["params"] = {}

        if "sinceTimeUtc" in req_args["params"]:
            old_timestamp = req_args["params"]['sinceTimeUtc']
        else:
            old_timestamp = '1970-01-01T00:00:00.0000000Z'

        if response_type == "json":
            output = json.loads(raw_response_output)

            for entry in output:
                temp_timestamp = entry.get("LastProcessedTimeUtc")
                if temp_timestamp > old_timestamp:
                    old_timestamp = temp_timestamp
                    req_args["params"]["sinceTimeUtc"]=old_timestamp

                print_xml_stream(json.dumps(entry),handle)
        else:
            print_xml_stream(raw_response_output,handle)

#HELPER FUNCTIONS
    
# prints XML stream
def print_xml_stream(s,handle=sys.stdout):
    print >>handle,  "<stream><event unbroken=\"1\"><data>%s</data><done/></event></stream>" % encodeXMLText(s)



def encodeXMLText(text):
    text = text.replace("&", "&amp;")
    text = text.replace("\"", "&quot;")
    text = text.replace("'", "&apos;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    text = text.replace("\n", "")
    return text
