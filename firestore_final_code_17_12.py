###data upload using function
import os
from os import path
import csv
import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore

cred = credentials.Certificate("C:\\Users\\shreya_s\\Desktop\\firebase\\firestore-4dc04-firebase-adminsdk-t7qh8-470fa43d4d.json")
app = firebase_admin.initialize_app(cred)
store = firestore.client()

no_of_sheets=int(input('enter no. OF SHEETS to be uploaded in database'))
sub_collection_name=[]
while (no_of_sheets >0) :
    sub_collection_name.append(input('enter sheet name'))
    no_of_sheets= no_of_sheets -1
collection_name='Shreya_TEST1'
document_name= 'Doc1'
#sub_collection_name='Data2'
for i in sub_collection_name:
    #file_path="C:\\Users\\shreya_s\\Desktop\\data2.csv"
    file_path=os.path.join("C:\\Users\\shreya_s\\Desktop\\", i + ".csv")
    def batch_data(iterable, n):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]


    data = []
    headers = [] #listf
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                for header in row:
                    headers.append(header)
                line_count += 1
            else:
                #dictionary
                obj = {}
                for idx, item in enumerate(row):
                    obj[headers[idx]] = item
                data.append(obj)
                line_count += 1
        print(f'Processed {line_count} lines.')

    count = 0
    for batched_data in batch_data(data, 499):
        batch = store.batch()

        for data_item in batched_data:
            count = count + 1
            value1 = str(count)
            doc_ref = store.collection(collection_name).document(document_name).collection(i).document(value1)
            #/ Panda / Data / 18 / mrpmwwAHVv6LTBGpOpDe
            batch.set(doc_ref, data_item)
        batch.commit()

    print('Done')

############################################
######document name under parent document
qwerty=store.collection('Shreya_TEST').list_documents()
for k in qwerty:
    document_name=k.id
    print(k.id)
###### sub collections under parent document
c=store.collection('Shreya_TEST').document(document_name).collections()
sub_coll_list=[]
for ss in c:
    #sub_collection_name=ss.id
    print(ss.id)
    sub_coll_list.append(ss.id)
######listoflists for fields of different sheets
dep=[]
for count,abcd in enumerate(sub_coll_list):
    collection_ref = store.collection('Shreya_TEST').document(document_name).collection(abcd)
    query = collection_ref.where('serial_no', '==', '1')
    result = query.stream()
    dep.append([])
    for doc in result:
        s=doc.to_dict()
    for k in s.keys():
        dep[count].append(k)
###################################################
####################################################################
#NOTE: in the above code, collection_ref is set to data2 or the last sheet that has been refereed
#########now segregate data on the basis of which keys of input (key value-- dictionary) which belongs to which sheet
ff={'TRACK': 'Windows', 'TRACK GROUP': 'WINTEL DOMAIN', 'Workload': '0.5', 'Operator': '>='}
input_keys=[]
for i in ff.keys():
    input_keys.append(i)
where_val1=[]
where_val2=[]
for j,k in enumerate(dep):
    for l in input_keys:
        if l in dep[j] and j==0:
            where_val1.append(l)
        ####dep[0] is sheet1 or data1
          #  q=store.collection('Shreya_TEST').document(document_name).collection('data1')
          #  q_stream= q.stream()
        elif l in dep[j] and j==1:
            where_val2.append(l)
          #  q = store.collection('Shreya_TEST').document(document_name).collection('data2')
          #  q_stream = q.stream()

##############dynamic query##########
q1 = store.collection('Shreya_TEST').document(document_name).collection('data1')
q2 = store.collection('Shreya_TEST').document(document_name).collection('data2')
#q=q1
for x, y in enumerate(where_val2):
    #########x is the index.. 0,1,2 | y is the value in where_val2
    #########
     if x==0:
        ##ff['operator'] has operator..
        ##q='q' q variable storing 'q' variable
        ##k stores static 'query'+x in form of str.. ---> gives query0, query1 etc
        op = ff['Operator']
        q = 'q'
        k = 'query' + str(x)
        ##if the input is workload then the operator is to be passe according to the query
        ##y has the field's name
        ##ff[y] has the value corresponding to y (field's name)
        ##v is having the query including field name operator and value corresponding to y
        ###q1 q2 is being selected on the basis of the fieldname present in which sheet-- where_val1 or where_val2
        if y == 'Workload':
            v = "q2.where(y, op, ff[y])"
        else:
            v = "q2.where(y, '==', ff[y])"
        ##exec-- executes the data that is stored in the variables
        ##k is having --query0 (string)
        ##v is having query in form of string
        ##exec will run k,v assign output of v(query) in query0 which is stored in k
        ##exec2 will run q,k  |||||| q has string value 'q' k has query0 that has output of original query/.
        ##exec2q would be having original query.. this has been done so that we can use . operator to apply second query
        exec("%s=%s" % (k, v))
        exec("%s=%s" % (q, k))

    else:
        op = ff['Operator']
        k1='query'+ str(x)
        if y == 'Workload':
            v1 = "q.where(y, op, ff[y])"
        else:
            v1 = "q.where(y, '==', ff[y])"
        exec("%s=%s" % (k1,v1))
        q='q'
        exec("%s=%s" % (q,k1))
##result of final query stream
res1 = q.stream()
for doc1 in res1:
    print('{} => {}'.format(doc1.id, doc1.to_dict()))

########1-- dynamic for both lists whereval_1 and 2
########2 handle workload field,, seperate condition for workload ------- done
########3 case sensitivity