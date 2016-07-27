import sys
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: http_requests_streaming <some string>"
        exit(-1)
        
    sc = SparkContext()
    
    
    input_string = sys.argv[1]
    
    print "The input is:", input_string 
    
    
    # End of the application
    sc.stop()