import argparse,csv,json,re,os,multiprocessing
from collections import OrderedDict
from concurrent import futures
class converter():
 def __init__(self,conv_size,conv_line ,conv_header ,conv_type ,conv_input ,conv_output):
  self.conv_line = conv_line
  self.conv_header = conv_header
  self.conv_type = conv_type
  self.conv_input = conv_input
  self.conv_output = conv_output
  self.conv_size = conv_size
  self.num = 0
  self.lineNumber = 0
  self.size = self.size_manager()
  self.fileToRead = self.input_manager()
  self.output_file = self.output_manager()
  self.header = self.header_manager()
  print(self.size)
#-----------------------------------------------------  
 def input_manager(self):
  try :
   input_file = open(self.conv_input,'rt')
  except Exception as e:
   print('An error acure during opening input file(%s), Error is :==>> %s' %(conv_input,e)) 
   raise SystemExit
  else:
    if self.conv_type == 'csv':
     return csv.reader(input_file)

#-----------------------------------------------------
 def output_manager(self):
  if self.conv_type == 'csv':
   self.output_extention = '.json'
  else :
   self.output_extention = '.csv'

  if self.conv_line or self.size:
   self.num +=1
   self.outputName = self.conv_output + '_' + str(self.num) + self.output_extention
  else:
   self.outputName = self.conv_output + self.output_extention

  try :
   output_file = open(self.outputName,'at', encoding='UTF-16')
  except Exception as e:
   print('An error acure during creating output file(%s), Error is : %s' %(outputName,e)) 
   raise SystemExit

  return output_file

#-----------------------------------------------------
 def header_manager(self):
  if not self.conv_header:
   return next(self.fileToRead)
  else:
   headerList = self.conv_header.split(',')
   input_file = open(self.conv_input,'rt') 
   input_file_csv = csv.reader(input_file)
   lenHeaderList = len(headerList)
   lencsvFile = len(next(input_file_csv))
   if lencsvFile == lenHeaderList:
    input_file.close()
    return headerList
   else:
    print("lengh of provided header is not equal to csv file (( %s != %s )) fields, please check header : %s"%(lenHeaderList,lencsvFile,headerList))
    input_file.close()    
    raise SystemExit

#-----------------------------------------------------

 def size_manager(self):
  extention = ['b','kb','mb','gb']
  if not self.conv_size:
   return False
  else: 
   regex_alphabet = re.compile(r'[a-zA-Z]+')
   regex_digit = re.compile(r'\d+')
   regex_size = regex_digit.search(self.conv_size)
   regex_size_extention = regex_alphabet.findall(self.conv_size)
   if regex_size and regex_size_extention:
    size = int(regex_size.group())
    size_extention = regex_size_extention[0]
   else:
    print('error in size, please check size extention, valid extention %s' %(extention))
    raise SystemExit    

   if size_extention in extention and size > 0:
    if  size_extention == 'b':
     return size
    elif size_extention == 'kb':
     return size * 1000
    elif size_extention == 'mb':
     return size * 1000000
    elif size_extention == 'gb':
     return size * 1000000000
   else:
    print('error in size, please check size extention, valid extention %s' %(extention))
    raise SystemExit
#-----------------------------------------------------
 def conv_save_csvTOjson(self): 
  flag = True 
  while flag ==True:
   try :
    dictGeneratorPerLine = OrderedDict(zip(self.header,next(self.fileToRead)))
   except StopIteration as SI:
    flag = False
    self.output_file.flush()
    self.output_file.close()
    print('end of file',SI)
    raise SystemExit    
   else:
    if self.conv_line:
     if self.lineNumber == int(self.conv_line):
      self.output_file.flush()
      self.output_file.close()
      self.output_file = self.output_manager()
      self.lineNumber = 0
    if self.conv_size:
     if os.path.getsize(self.outputName) >= self.size:
      self.output_file.flush()
      self.output_file.close()
      self.output_file = self.output_manager()
      self.lineNumber = 0
       
   try :
    jstring = json.dumps(dictGeneratorPerLine)
   except Exception as e:
    print('An error acured during converting %s line to Json string' %(dict(dictGeneratorPerLine)))
    
   try :
    self.output_file.write(jstring)
    self.output_file.write("\n")
#    self.output_file.flush()
    self.lineNumber += 1
   except Exception as e:
    print('somthing gone wronge during appending: %s' %(dict(dictGeneratorPerLine)))  

#-----------------------------------------------------

#-----------------------------------------------------
if __name__ == "__main__":
 parser = argparse.ArgumentParser(description = "csvTojson convertor")
 parser.add_argument(
  '-s',
  '--size',
  default = False,
  help = "size limit per output file Extention must be in: 'b,kb,mg,gb' 1kb is 1000byte "
  )
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
 b = converter(
  conv_size = args.size,
  conv_line = args.line,
  conv_header = args.header,
  conv_type = args.type,
  conv_input = args.input,
  conv_output = args.output
   )

# for i in range(5):
#  p = multiprocessing.Process(target=b.conv_save_csvTOjson())
#  p.start()

 b.conv_save_csvTOjson()
# with futures.ProcessPoolExecutor(max_workers=4) as executor:
#  executor.submit(b.conv_save_csvTOjson())
#  executor.submit(b.conv_save_csvTOjson())  
#  executor.submit(b.conv_save_csvTOjson())
#  executor.submit(b.conv_save_csvTOjson())



