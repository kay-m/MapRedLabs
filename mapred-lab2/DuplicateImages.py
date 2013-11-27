import MapReduce
import sys

"""
This application is a map reduce implementation to find duplicate unages in a 
corpus of small images.
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 
#  Application Usage:
#
#      python DuplicateImages.py arg-1 
#
#      arg-1 : File containing list of images to be processed. Each line of the 
#              file should contain one image file name (including path)
#
#  Dataset: imagelist.txt (in the same folder as this file)
#  
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  
#  Algorithm Design:
#  
#  Key Idea: Use the pixel array representation of the full image as a key. 
#            Send all images with the same pixel array representation to the 
#            same reducer.
#
#  Mapper:
#  1. Input:  List containing two elements. The first is the name of the image
#             file. The second element is a list in itself containing the pixel
#             values of all the channels in the image. 
#  2. Key   : String representation of image data
#     value : file name
#     Do not emit anything if the AnswerCount field is NULL. 
#  Reducer:
#     Emit an output only if the key is zero.     
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++       

"""
  Questions:
  1. 
     
  Extensions:
  1. Extend the application to use a more efficient metric to compare two images. 
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def mapper(key,record):
  # record : List with two elements. 
  #   0. File name 
  #   1. Image data as a list. 
  
  # Extract fileName and imageData from the record.
  
  
  # key -> string representation of imageData, value -> fileName
  

def reducer(key, list_of_values):
  
  # Extract each element from list_of_values (it is a python list) and append
  # it to a string and emit the string.
  

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def main():
  # Build list of image files to be processed
  fileNameList = []
  inputFiles = open(sys.argv[1])
  for line in inputFiles:
    fileNameList.append(line.strip())
    
  mr.execute(fileNameList, mapper, reducer,"IMAGE")
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++  
if __name__ == '__main__':
    main()
  
