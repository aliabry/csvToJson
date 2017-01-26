import argparse,csv,json
from collections import OrderedDict
def converter(conv_line,conv_header,conv_type,conv_input,conv_output):
 #output_file = conv_output
 input_file = input_manager(conv_input)

 if conv_type == 'csv':
  csvFile = csv.reader(input_file)  
  header = header_manager(conv_input,csvFile,conv_header)
  conv_save(header,csvFile,conv_output,conv_line)

#--------------------------------------------------
def input_manager(conv_input):
 try :
  input_file = open(conv_input,'rt')
 except Exception as e:
  print('An error acure during opening input file(%s), Error is :==>> %s' %(conv_input,e)) 
  raise SystemExit
 else:
  return input_file

def output_manager(conv_output,num):
 try :
  if num:
   outputName = conv_output + '_' + str(num) + '.json'
  else:
   outputName = conv_output + '.json'
  output_file = open(outputName,'at', encoding='UTF-16')
 except Exception as e:
  print('An error acure during creating output file(%s), Error is : %s' %(outputName,e)) 
  raise SystemExit

 return output_file
#--------------------------------------------------
def header_manager(conv_input,csvFile,conv_header):
 if not conv_header:
  return next(csvFile)
 else:
  headerList = conv_header.split(',')
  input_file = open(conv_input,'rt') 
  input_file_csv = csv.reader(input_file)
  lenHeaderList = len(headerList)
  lencsvFile = len(next(input_file_csv))
  if lencsvFile == lenHeaderList:
   return headerList
   input_file.close()
  else:
   print("lengh of provided header is not equal to csv file (( %s != %s )) fields, please check header : %s"%(lenHeaderList,lencsvFile,headerList))
   input_file.close()    
   raise SystemExit
#----------------------------------------------------

def conv_save(header,csvFile,conv_output,conv_line):
 flag = True

 if conv_line:
  num = 1
 else:
  num = 0

 lineNumber = 0
 output_file = output_manager(conv_output,num)
 while flag ==True:
  try :
   dictGeneratorPerLine = OrderedDict(zip(header,next(csvFile)))
  except StopIteration as SI:
   flag = False
   output_file.flush()
   output_file.close()
   print('end of file',SI)
   raise SystemExit    

  try :
   jstring = json.dumps(dictGeneratorPerLine)
  except Exception as e:
   print('An error acured during converting %s line to Json string' %(dict(dictGeneratorPerLine)))
   
  try :
   output_file.write(jstring)
   output_file.write("\n")
   output_file.flush()
   lineNumber += 1
  except Exception as e:
   print('somthing gone wronge during appending: %s' %(dict(dictGeneratorPerLine)))  

  if lineNumber & lineNumber == int(conv_line):
   num += 1
   output_file.flush()
   output_file.close()
   output_file = output_manager(conv_output,num)
   lineNumber = 0

#---------------------------------------------------

if __name__ == "__main__":
 parser = argparse.ArgumentParser(description = "csvTojson convertor")
 parser.add_argument(
  '-H',
  '--header',
  required = False,
  default = False,
  help = 'header for csv file format only separated by "," '
 )
 parser.add_argument(
  '-l',
  '--line',
  default = 0,
  type = int,
  help = "limiting the number of files per outputfile,output file will numbered from one to N"
  )
 parser.add_argument(
  '-t',
  '--type',
  choices=["csv","json"],
  required = True,
  help = "the format that input file will be converte to"
  )
 parser.add_argument(
  '-i',
  '--input',
  required = True,
#  type = argparse.FileType('rt'),
  help = "input file to be converte"
  )
 parser.add_argument(
  '-o',
  '--output',
#  type = argparse.FileType('at',encoding='UTF-16'),
  help = "output file for saving converted files"
  )
 args = parser.parse_args()
 converter(
  conv_line = args.line,
  conv_header = args.header,
  conv_type = args.type,
  conv_input = args.input,
  conv_output = args.output
   )

