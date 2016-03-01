from pyspark import SparkContext, SparkConf
import time
import sys

conf = SparkConf().setAppName("WC-Tasks")
sc = SparkContext(conf = conf)
fileName = "s3n://assignment1-dataset/"
#fileName = "bible+shakes.nopunc"

inputList = "but peter took him up saying stand up i myself also am a man and as he talked with him he went in and found many that were come together and he said unto them ye know how that it is an unlawful thing for a man that is a jew to keep company or come unto one of another nation but god hath shewed me that i should not call any man common or unclean therefore came i unto you without gainsaying as soon as i was sent for i ask therefore for what intent ye have sent for me and cornelius said four days ago i was fasting until this hour and at the ninth hour i prayed in my house and behold a man stood before me in bright clothing"

# Output directories of each of the tasks
dir1 = "s3n://assignment1-output/task1-output/out"
dir2 = "s3n://assignment1-output/task2-output/out"
dir3 = "s3n://assignment1-output/task3-output/out"

# Boolean value determining which tasks to run
runTask1 = True
runTask2 = True
runTask3 = True

def fancyPrint(pair):
    count = pair[0]
    word = pair[1]
    #sys.stdout.write(word + " : " + str(count))
    print ("%s : %i" % (word, count))

def wordCount(words):
    # Reverse the key value pair and sort by word count value.
    counts = words \
            .map(lambda x: (x,1)) \
            .reduceByKey(lambda x,y : x+y)\
            .map(lambda (x,y):(y,x)) \
            .sortByKey(ascending = True)
    counts.saveAsTextFile(dir1)
    return counts

# Task 2
def doubleWordCount(words):
    words2 = words.zipWithIndex()
    words2 = words2.map(lambda (x,y) : (y,x))
    words1 = words2.map(lambda (x,y) : (x+1,y))

    doublewords = words1.union(words2) \
                    .reduceByKey( lambda x,y : x+ " "+ y ) \
                    .map( lambda (x,y) : (y,1)) \
                    .reduceByKey(lambda x,y : x+y) \
                    .map(lambda (x,y) : (y,x)) \
                    .sortByKey()
    doublewords.saveAsTextFile(dir2)
    return doublewords

# Task 3
def listWordCount(words):
    # Split the input words as a list
    inputWords = inputList.split()
    '''
    Map the words from dataset and do a reduceByKey by them to find the count
    of each word. After the count is found, do a filter searching only for
    those words that are in the input list. sortByKey is optional
    '''
    result = words \
            .map(lambda x : (x,1))\
            .reduceByKey(lambda x,y : x+y) \
            .filter(lambda x : x[0] in inputList)\
            .map(lambda (x,y) : (y,x)) \
            .sortByKey()

    result.saveAsTextFile(dir3)
    return result

def doubleWordCountPrevious(words):

    result = {}
    first = ""
    second = ""
    allWords = words.collect()

    # Look for all possible double words in the collection and
    # increment a dict key for each unique key found
    for word in allWords:
        if second :
           first = second
           second = word
           newWord = first + " " + second
           if newWord in result:
               result[newWord] += 1
           else:
               result[newWord] = 1
        elif first :
            second = word
            newWord = first + " " + second
            result[newWord] = 1
        else:
            first = word
    output = sc.parallelize(result)
    # Sort by the count value and save the result
    # output = output.sortBy(lambda x: x[1])
    output.saveAsTextFile(dir2 + "old")
    return output

if __name__ == "__main__":
    # Read the text file.
    lines = sc.textFile(fileName)

    # Split the file as words
    words = lines.flatMap(lambda x: x.split()).persist()

    t1 = time.time()
    if runTask1:
        # Do the task 1.
        output1 = wordCount(words)
        t1 = time.time() - t1
        print output1.collect()
        #output1.foreach(fancyPrint)
    else:
        t1 = time.time() - t1

    print ("###################################\n###################################")

    t2 = time.time()
    if runTask2:
        # Do the task 2 using the same rdd.
        output2 = doubleWordCount(words)
        t2 = time.time() - t2
        print output2.collect()
        #output2.foreach(fancyPrint)
    else:
        t2 = time.time() - t2

    print ("###################################\n###################################")

    t3 = time.time()
    if runTask3:
        # Do the task 3 using the same rdd.
        output3 = listWordCount(words)
        t3 = time.time() - t3
        print output3.collect()
        #output3.foreach(fancyPrint)
    else :
        t3 = time.time() - t3

    print ("###################################\n###################################")

    print ("Task1 Total time: " , t1)
    print ("Task2 Total time: " , t2)
    print ("Task3 Total time: " , t3)
