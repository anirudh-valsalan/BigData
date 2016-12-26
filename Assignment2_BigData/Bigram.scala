
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

def textToLemmas(text: String): Seq[String] = {
    val elem="a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your"
    val stopWords=elem.split(",")
	val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
	  
	  
      if (lemma.length > 2 && !stopWords.contains(lemma)) {
        lemmas += lemma.toLowerCase
      }
	  else if(stopWords.contains(lemma))
	  {
	   lemmas += "be"
	  }
    }
    lemmas
  }
val text =  sc.parallelize(List("Alice is testing spark application. Testing spark is fun"))
val postLemmatize = text.map(textToLemmas(_))

for(i<-postLemmatize){
  var rowMap:Map[String,Int] = Map(); 
  for(ind<-0 to i.length-2){
	    val outText ="("+i(ind).toLowerCase()+","+i(ind+1).toLowerCase()+")"
		if(!rowMap.keySet.contains(outText)){
			rowMap+=(outText->1)
		
		}
		else{
		 var value=(rowMap(outText));
			 value+=1;
			 rowMap+=(outText->value)
		}
		
		if(ind==i.length-2){
		
		val out = ListMap(rowMap.toSeq.sortBy(-_._2):_*).take(1);
		println(out)}
  }
	
  }
