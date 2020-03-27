import os
import slack
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.query import SimpleStatement
IP_LIMS = '192.168.10.2'
PASSWORD = 'accugene1027!'
KEYSPACE_LIMS = 'acculims_new'
ap = PlainTextAuthProvider(username='accugene', password=PASSWORD)

def find_barcode_by_samid(sam):
    c = Cluster([IP_LIMS], auth_provider=ap)
    s = c.connect()
    s.row_factory = dict_factory
    s.set_keyspace(KEYSPACE_LIMS)

    barcode_checked = []
    msg = ''
    barcode = ''
    state = ''
    #sample id 꼬인 데이터 체크
    rows = s.execute(SimpleStatement("select * from sample where id = %s allow filtering"), (sam,))
    flag=True
    for row in rows:
        flag=False
        barcode_inrow = str(row['barcode'])
        if barcode_inrow in barcode_checked:
            msg = '중복 바코드 존재 in new_barcodes => ' +barcode_inrow  + ' sam => ' + sam
        else:
            barcode_checked.append(barcode_inrow)
        barcode = barcode_inrow
        state = row['state']

    if flag:
        msg = '정보가 존재하지 않습니다. ==> ' + sam
    s.shutdown()
    c.shutdown()
    return barcode, state, msg

def find_geninus_status_by_date(year,month,day):
    c = Cluster([IP_LIMS], auth_provider=ap)
    s = c.connect()
    s.row_factory = dict_factory
    s.set_keyspace(KEYSPACE_LIMS)
    result_list = []
    rows = s.execute(SimpleStatement("select * from sample_group where group='Sample' and year= %s and month = %s allow filtering"), (year,month,))
    for row in rows:
        if '지니너스' in row['etc']['접수명']:
            tstmp = int(row['etc']['접수일'])/1000
            result_y = int(datetime.fromtimestamp(tstmp).strftime('%Y'))
            result_m = int(datetime.fromtimestamp(tstmp).strftime('%m'))
            result_d = int(datetime.fromtimestamp(tstmp).strftime('%d'))
            groupid=row['groupid']
            if year == result_y and month == result_m and day == result_d:
                rows2 = s.execute(SimpleStatement("select * from sample where group=%s"),(row['groupid'],))
                for row2 in rows2:
                    result_list.append([groupid, row2['id'], row2['barcode'], row2['state']])
    return result_list



@slack.RTMClient.run_on(event='message')
def say_hello(**payload):
    data = payload['data']
    web_client = payload['web_client']
    rtm_client = payload['rtm_client']
    if 'Hello' in data.get('text', []):
        channel_id = data['channel']
        thread_ts = data['ts']
        user = data['user']

        web_client.chat_postMessage(
            channel=channel_id,
            text=f"Hi <@{user}>!",
            thread_ts=thread_ts
        )
    if '!help' in data.get('text', []):
        channel_id = data['channel']
        return_message = '====명령어 목록====\n\
            1. !runlist : 최근 5개 런 목록(runid, 런완료여부)을 보여준다.\n\
            2. !runsamplelist_runid : 해당 runid에 포함된 샘플리스트(샘플이름)를 보여준다.\n\
            3. !geninus_20200304 : 해당 접수일에 포함된 지니너스 샘플의 진행 상태를 보여준다.'
        web_client.chat_postMessage(
            channel=channel_id,
            text=return_message
                )

    elif '!geninus' in data.get('text', []) and ':' not in data.get('text',[]):
        input = data.get('text', [])
        channel_id = data['channel']
        thread_ts = data['ts']
        if '_' not in input or not input.split('_')[1].isdigit():
            web_client.chat_postMessage(channel=channel_id, text='해당 지니너스 정보가 존재하지 않거나 입력이 올바르지 않습니다.')
        if len(input.split('_')[1]) != 8:
            web_client.chat_postMessage(channel=channel_id, text='접수일을 8자리 형식으로 입력 해 주세요. 예:20200304')
            return
        date = input.split('_')[1]
        year = int(date[0:4])
        month = int(date[4:6])
        day = int(date[6:8])
        result_list = find_geninus_status_by_date(year,month,day)

        return_message = '===='+date+'조회결과====\n'
        count=0
        for result in result_list:
            count+=1
            return_message+=str(count)+'. GROUPID=>'+result[0]+' SAMID=>'+result[1]+' BARCODE=>'+result[2]+' STATUS =>'+result[3]+'\n'
        web_client.chat_postMessage(channel=channel_id, thread_ts = thread_ts,
                text = return_message)



    elif '!runsamplelist' in data.get('text', []) and ':' not in data.get('text', []):
        channel_id = data['channel'] 
        thread_ts=data['ts']
        if '_' not in data.get('text', []):
            return_message='해당 runid를 찾을 수 없습니다.'
            web_client.chat_postMessage(
                channel=channel_id,
                text=return_message
                )
        else:
            runid = '_'.join(data.get('text', []).split('_')[1:])
            path = os.path.join('/data/lims/device/MiSeqDx01',runid)
            return_message='====='+runid+' 샘플 리스트====='
            web_client.chat_postMessage(
                channel=channel_id,
                text=return_message,
                thread_ts=thread_ts
                )

            if os.path.isdir(path):
                samplesheet = os.path.join(path, 'SampleSheet.csv')
                
                if os.path.isfile(samplesheet):

                    dataline_flag = False
                    sam_list = []
                    with open(samplesheet,'r') as f:
                       
                        while True:
                            line = f.readline()
                            if not line:
                                break
                            
                            if dataline_flag:
                                sam_list.append(line.split(',')[1])    

                            if '[Data]' in line:
                                dataline_flag = True
                                f.readline()
                    count=0
                    for samid in sam_list:
                        count+=1
                        sam = samid.split('-')[0]
                        barcode, state, msg = find_barcode_by_samid(sam)
                        post_msg = ''
                        if msg:
                            post_msg=' 비고=>'+msg
                        return_message=str(count)+'. SampleID=>'+samid+' Name=>'+barcode+post_msg
                        web_client.chat_postMessage(
                        channel=channel_id,
                        text=return_message,
                        thread_ts=thread_ts
                        )
                    web_client.chat_postMessage(
                            channel=channel_id,
                            text='데이터 카운트=>'+str(len(sam_list)),
                            thread_ts=thread_ts
                    )
        

        

    elif '!runlist' == data.get('text', []):
        channel_id = data['channel']
        thread_ts = data['ts']
        user = data['user']
        
        folder_path ='/data/lims/device/MiSeqDx01/'
        # each_file_path_and_gen_time: 각 file의 경로와, 생성 시간을 저장함
        each_file_path_and_gen_time = []
        for each_file_name in os.listdir(folder_path):
            # 런 폴더 형식이 아닌 것은 패스
            if '_' not in each_file_name or '.txt' in each_file_name:
                continue

            # getctime: 입력받은 경로에 대한 생성 시간을 리턴
            each_file_path = folder_path + each_file_name
            each_file_gen_time = os.path.getctime(each_file_path)
            each_file_path_and_gen_time.append(
                (each_file_name, each_file_gen_time)
            
            )
        return_str = '====최근 5개 런 목록====\n'
        for i in range(0,5):
            most_recent = max(each_file_path_and_gen_time, key=lambda x: x[1])
            check_complete = '미완료'
            if 'RTAComplete.txt' in os.listdir(folder_path+most_recent[0]):
                check_complete = '완료'
            return_str += most_recent[0]+ ', ' + check_complete + '\n'
            each_file_path_and_gen_time.remove(most_recent)
        
        web_client.chat_postMessage(
                channel=channel_id,
                text=return_str
                )

err_count=0
#slack_token = os.environ["SLACK_BOT_TOKEN"]
#rtm_client = slack.RTMClient(token=slack_token)
#rtm_client.start()

#sys.exit()
while True:
    try:
        
        if err_count==10:
            break
        slack_token = os.environ["SLACK_BOT_TOKEN"]
        rtm_client = slack.RTMClient(token=slack_token)
        rtm_client.start()
    except Exception as ex:
        print('에러가 발생 했습니다.', ex)
        err_count+=1


