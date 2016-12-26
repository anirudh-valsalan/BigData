import nltk
import regex as re
from nltk.stem import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
myMountName = "python_first"
stemmer = PorterStemmer()

myRDDOut = sc.textFile("/mnt/%s/homework5.txt" % myMountName)\
    .flatMap(lambda z:z.split("."))\
    .map(lambda z:z.strip(" ").split())\
    .filter(lambda z:len(z)!=0)
myRDDOut.collect()
stopwordList = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your"

stemmOut = myRDDOut.map(lambda z:[re.sub('[^A-Za-z0-9]+','',z[i]).lower() for i in range(0,len(z))]) \
    .map(lambda z:[stemmer.stem(z[i]) for i in range(0,len(z))]) \
    .map(lambda z:[str(z[i]) for i in range(0,len(z))]) \
    .map(lambda z:['be' if(z[i] in stopwordList) else z[i] for i in range(0,len(z))])
bigramsOut = stemmOut.flatMap(lambda z:[((z[i],z[i+1]),1) for i in range(0,len(z)-1)])
resultOut = bigramsOut.reduceByKey(lambda z,y:z+y)\
    .filter(lambda z:z[1]>1)\
    .map(lambda z:('('+','.join(z[0])+')',str(z[1])))
result = resultOut.collect()
for k in range(0,len(result)):
 print " ".join(result[k])
 