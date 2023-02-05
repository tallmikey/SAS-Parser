#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import shutil
import re
import pandas 
import networkx as nx
from pandas.util.testing import equalContents
from fileinput import filename

def get_list_log(sp_path):
    script_list = list()
    for root, dirs, files in os.walk(sp_path):
        for file in files:
            if file.lower().endswith(".log"):
                 script_list.append(os.path.join(root, file))
    return script_list


class SASLogComponent():
    def __init__(self,start_line, end_line, contents):
        self.start_line = start_line
        self.end_line = end_line
        self.contents = contents

class Note(SASLogComponent):
    """Note class
    This is short version that is only processing the essential elements for the purpose of data table level lineage.
    Other information are processed by NOTE_fullver class.
    INPUT:  starting line, ending line, contents
    OUTPUT: 
            TYPE:       INPUT, OUTPUT, DATASTEP, PROC, LIBREFDEASSIGN, LIBREFASSIGN
            DATA_NAME:  Dataset/data table name
            RESNAME:    Resource name
            END_PROC:   Flag to indicate whether current note ends a SAS procedure/data step.
    """
    def __init__(self,start_line, end_line, contents):
        super().__init__(start_line, end_line, contents)
        self.Type = ""
        self.data_name = ""
        #self.data_out = ""
        self.ResName = ""
        self.End_Proc = False
        
        #Input type
        reg_exp = re.compile(r"(?i)(?:^NOTE:.*observations\s+read\s+from\s+the\s+data\s+set\s+([a-zA-Z_&][a-zA-Z0-9_&]{0,31}\.[a-zA-Z_&][a-zA-Z0-9_&]{0,31}))")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "INPUT"
            self.data_name = re.search(reg_exp, self.contents).group(1)
            self.ResName =""
            
        #Input type
        reg_exp = re.compile(r"(?i)(?:^NOTE:\s+No\s+observations\s+in\s+data\s+set\s+([a-zA-Z_&][a-zA-Z0-9_&]{0,31}\.[a-zA-Z_&][a-zA-Z0-9_&]{0,31}))")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "INPUT"
            self.data_name = re.search(reg_exp, self.contents).group(1)
            self.ResName =""
            
        #Output type
        reg_exp = re.compile(r"(?i)(?:^NOTE:\s+The\s+data\s+set\s+([a-zA-Z_&][a-zA-Z0-9_&]{0,31}\.[a-zA-Z_&][a-zA-Z0-9_&]{0,31}))\s+has")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "OUTPUT"
            self.data_name = re.search(reg_exp, self.contents).group(1)
            self.ResName =""
            
    
        #Data step
        reg_exp = re.compile(r"(?i)(?:^NOTE:\s+DATA\s+statement\s+used\s+)")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "DATASTEP"
            self.data_name = ""
            self.ResName =""
            self.End_Proc = True
            
        #Data step
        reg_exp = re.compile(r"(?i)(?:^NOTE:\s+PROCEDURE\s+([a-zA-Z]+)\s+used\s+)")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "PROC " + re.search(reg_exp, self.contents).group(1)
            self.data_name =  ""
            self.ResName =""
            self.End_Proc = True
            
        #Infile 1
        reg_exp = re.compile(r"(?i)(?:^NOTE:.*\s+read\s+from\s+the\s+infile\s+([a-zA-Z_&][a-zA-Z0-9_&]{0,31}))")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "INPUT"
            self.data_name = re.search(reg_exp, self.contents).group(1)
            self.ResName =""
            
            
        #libref deassign
        reg_exp = re.compile(r"(?i)(?:^NOTE:.*\s+Libref\s+([a-zA-Z_&][a-zA-Z0-9_&]{0,31}))\shas\s+been\s+deassigned")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "LIBREFDEASSIGN"
            self.data_name = re.search(reg_exp, self.contents).group(1)
            self.ResName =""
            self.End_Proc = True
            
        #libref assign
        reg_exp = re.compile(r"(?i)(?:^NOTE:.*\s+Libref\s+([a-zA-Z_&][a-zA-Z0-9_&]{0,31}))\s+was\s+successfully\s+assigned\s+as\s+follows:")
        if re.search(reg_exp, self.contents) != None:
            self.Type = "LIBREFASSIGN"
            self.data_name = re.search(reg_exp, self.contents).group(1)
            self.ResName =""
            self.End_Proc = True
            
        #Infile 2
        """
        reg_exp = re.compile(r"(?i)(?:^NOTE:.*\s+read\s+from\s+the\s+infile\s+([a-zA-Z_&][a-zA-Z0-9_&]{0,31}))")
        if re.search(reg_exp, self.contents) != None:
            self.type = "INPUT"
            self.data_name = re.search(reg_exp, self.contents).group(1)
            self.ResName =""
        """    
        if re.search(r"(?i)(?:^NOTE:.*%INCLUDE\s+)", self.contents) !=None:
            self.End_Proc = True
            
class Note_fullver(SASLogComponent):
    """Note Class 
    This is the full version written by Agnieszka. Currently it is not used by the main program.
    INPUT: starting line number, endding line number, contents
    OUTPUT: Note Type, Input Data, Output data, additional resource name
    """
      
    def __init__(self,start_line, end_line, contents):
        super().__init__(start_line, end_line, contents)
        parsed_data = self.parse_contents()
        self.note_type = parsed_data['note_type']
        self.data_input = parsed_data['data_input']
        self.data_output = parsed_data['data_output']
        self.resource = parsed_data['resource']

    ## Getting file location and it's refname:    
    def get_fileref_resource(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(3)
        except:
            pass
        return resource
    
    def get_fileref_data_out(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(2)
        except:
            pass
        return resource
    
    ## Getting library reference and library libref:
    def get_libref_resource(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(2)
        except:
            pass
        return resource
    
    def get_libref_data_out(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(1)
        except:
            pass
        return resource
    
    ## Getting data input for non-empty sas datasets:
    def get_read_data_in_non_empty(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(1)
        except:
            pass
        return resource
    
    def get_read_data_in_empty(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(1)
        except:
            pass
        return resource
    
    def get_stat_name(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(1)
        except:
            pass
        return resource
    
    def get_file_name(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(1)
        except:
            pass
        return resource
    
    def get_write_not_empty(self,  regex):
        resource = ""
        try:    
            print (re.search(regex,self.contents).group(1))
            resource = re.search(regex,self.contents).group(1)
        except:
            pass
        return resource
    
    def get_write_empty(self,  regex):
        resource = ""
        try:    
            resource = re.search(regex,self.contents).group(1)
        except:
            pass
        return resource
    
    def parse_contents(self):

        ### Define REGEX for each type of note - except MISC:
        
        ## NOTE: PROCEDURE SORT used (Total process time):
        ## NOTE: DATA statement used (Total process time):
        stat_re = re.compile("^NOTE:\s(PROCEDURE\s[A-Z]*|DATA\sstatement)\sused\s.*:")
        
        
        ## NOTE: Libref LIBRARY was successfully assigned as follows:  
        libref_re = re.compile("^NOTE:\sLibref\s([a-zA-Z0-9_\.]{0,8})\swas\ssuccessfully\sassigned\sas\sfollows:\s*Engine:.*\s*Physical\sName:\s(.*)\s")
        
        ## NOTE: The infile FLT is:\n Filename=\\tork188\e$\Prod\SG_CCA\Ndsu_Data\JAMAICA\Jun2018X\FPM320,
        fileref_re = re.compile("^(NOTE:\sThe\sinfile\s([a-zA-Z0-9_\.]{0,8})\sis:)\s*(Filename=.*),")
        
        ## NOTE: There were 1 observations read from the data set XREF.ALMDTLIM.WHERE UPCASE(subdir)='JAMAICA';
        ## NOTE: No observations were selected from data set WORK.ERR7.
        ## NOTE: 48079 records were read from the infile FLT.
        read_not_empty_sas_re = re.compile("^NOTE:\sThere\swere\s[0-9]*\sobservations\sread\sfrom\sthe\sdata\sset\s([a-zA-Z0-9_\.]{0,7}[a-zA-Z0-9_\.]{0,31})\.")
        read_empty_sas_re = re.compile("^NOTE:\sNo\sobservations\swere\sselected\sfrom\sdata\sset\s([a-zA-Z0-9_\.]{0,8}[a-zA-Z0-9_\.]{0,32})\.")
        read_file_re = re.compile("^NOTE:\s[0-9]*\srecords\swere\sread\sfrom\sthe\sinfile\s([a-zA-Z0-9_\.]{0,7})\.")
        
        ## NOTE: The data set WORK.TRNS has 217 observations and 1 variables.
        ## NOTE: No observations in data set WORK.ERR6.
        write_not_empty_sas_re = re.compile("^NOTE:\sThe\sdata\sset\s([a-zA-Z0-9_\.]{0,8}[a-zA-Z0-9_\.]{0,32})\shas\s[0-9]*\sobservations\sand\s[0-9]*\svariables\.")
        write_empty_sas_re = re.compile("^NOTE:\sNo\sobservations\sin\sdata\sset\s([a-zA-Z0-9_\.]{0,8}[a-zA-Z0-9_\.]{0,32})\.")
        
            
        # Define fixed list of the Note Types that need to be parsed for input, output and resource
        # STATS: identifies a procedure or data statement statistics
        # LIBREF: Refers to succesfully assigned library.
        # FILEREF: Refers to flat file read. 
        # READ: Refers to a dataset or other file read.
        # WRITE: Refers to a dataset orother file produced. 
              
        regex_array = ({
          "re": stat_re,
          "type": "STATS",
          "data_in": '',
          "data_out": '',
          "resource": self.get_stat_name(stat_re)
        }, {
          "re": libref_re,
          "type": "LIBREF",
          "data_in": '',
          "data_out": self.get_libref_data_out(libref_re),
          "resource": self.get_libref_resource(libref_re)
        }, {
          "re": fileref_re,
          "type": "FILEREF",
          "data_in": "",
          "data_out": self.get_fileref_data_out(fileref_re),
          "resource": self.get_fileref_resource(fileref_re)
        }, {
          "re": read_not_empty_sas_re,
          "type": "READ",
          "data_in": self.get_read_data_in_non_empty(read_not_empty_sas_re),
          "data_out": '',
          "resource": ''
        }, {
          "re": read_empty_sas_re,
          "type": "READ",
          "data_in": self.get_read_data_in_empty(read_empty_sas_re),
          "data_out": '',
          "resource": ''
        }, {
          "re": read_file_re,
          "type": "READ",
          "data_in": self.get_file_name(read_file_re),
          "data_out": '',
          "resource": ''
        }, {
          "re": write_not_empty_sas_re,
          "type": "WRITE",
          "data_in": '',
          "data_out": self.get_write_not_empty(write_not_empty_sas_re),
          "resource": ''
        }, {
          "re": write_empty_sas_re,
          "type": "WRITE",
          "data_in": '',
          "data_out": self.get_write_empty(write_empty_sas_re),
          "resource": ''
        })
        
        for regex_item in regex_array:
            regex_method = regex_item["re"]
            regex_output = re.match(regex_method, self.contents)
            if regex_output:
                note_type = regex_item["type"]
                data_input = regex_item["data_in"]
                data_output = regex_item["data_out"]
                resource = regex_item["resource"]
                break
            else:
                note_type ="MISC"
                data_input = ""
                data_output = ""
                resource = ""
        return {
          "note_type": note_type,
          "data_input": data_input,
          "data_output": data_output,
          "resource": resource
        }
                   
class MacroGen(SASLogComponent):
    def __init__(self,start_line, end_line, contents):
        super().__init__(start_line, end_line, contents)


class Warning(SASLogComponent):
    def __init__(self,start_line, end_line, contents):
        super().__init__(start_line, end_line, contents)

class ScriptLine(SASLogComponent):
    def __init__(self,start_line, end_line, contents):
        super().__init__(start_line, end_line, contents)

class Misc(SASLogComponent):
    def __init__(self,start_line, end_line, contents):
        super().__init__(start_line, end_line, contents)

class SASLogProc(SASLogComponent):
    def __init__(self,start_line, end_line, contents, Type ):
        super().__init__(start_line, end_line, contents)
        #self.start_line = start_line
        #self.end_line = end_line
        #self.contents = contents
        self.ProcType = Type
        self.data_in = []
        self.data_out = []
        
        for note in contents:
            if note.Type.upper() == "INPUT": 
                self.data_in.append(note.data_name)
            elif note.Type.upper() == "OUTPUT":
                self.data_out.append(note.data_name)
            
    
        
class SASLog:
    def __init__(self, path):
        self.path = path
        with open(self.path, "r") as infile:
            self.log_lines = infile.readlines()
            self.log_length = len(self.log_lines)
            # Optional preprocess to merge message having multiple lines into one line
        self.log_messages = []
        self.component_index = []
        line_step = 0
        current_script_line = 0
        for line in self.log_lines:
            if re.match("NOTE: ",line) != None \
                or re.match("MACROGEN\(EXTRACT\):",line) != None\
                or re.match("WARNING: ", line) != None:
                self.component_index.append(line_step+ 1)
            elif re.match("\d+\s+", line) != None and int(re.match("\d+\s+", line).group(0)) >= current_script_line + 1:
                self.component_index.append(line_step+1)
                current_script_line = int(re.match("\d+\s+", line).group(0))

            line_step += 1

        for i in range(len(self.component_index)):
            if i != len(self.component_index) - 1:
                log_message = SASLogComponent(self.component_index[i]\
                                              ,self.component_index[i + 1] - 1\
                                              ,"".join(self.log_lines[self.component_index[i] - 1:self.component_index[i + 1] - 1]))
                self.log_messages.append(log_message)
            elif i == len(self.component_index) -1:
                log_message = SASLogComponent(self.component_index[i] \
                                              , 999999
                                              , "".join(
                        self.log_lines[self.component_index[i] - 1:]))
                self.log_messages.append(log_message)
        self.note_messages = []
        self.macro_gens = []
        self.warning_messages = []
        self.script_lines = []
        self.misc_messages = []
        for log_message in self.log_messages:
            if re.match("NOTE: ",log_message.contents) != None:
                note_message = Note(log_message.start_line, log_message.end_line, log_message.contents)
                self.note_messages.append(note_message)
            elif re.match("MACROGEN(EXTRACT):",log_message.contents) != None:
                macro_gen = MacroGen(log_message.start_line, log_message.end_line, log_message.contents)
                self.macro_gens.append(macro_gen)
            elif re.match("WARNING: ", log_message.contents) != None:
                warning_message = Warning(log_message.start_line, log_message.end_line, log_message.contents)
                self.warning_messages.append(warning_message)
            elif re.match("\d+\s+", log_message.contents) != None:
                script_line = ScriptLine(log_message.start_line, log_message.end_line, log_message.contents)
                self.script_lines.append(script_line)
            else:
                misc_message = Misc(log_message.start_line, log_message.end_line, log_message.contents)
                self.misc_messages.append(misc_message)

        
        this_procedure_notes = []
        self.SAS_procedures = []
        for note_message in self.note_messages:
            this_procedure_notes.append(note_message)
            if note_message.End_Proc == True:
                procedure_start_line = this_procedure_notes[0].start_line
                procedure_end_line = this_procedure_notes[-1].end_line
                SAS_procedure = SASLogProc(procedure_start_line,procedure_end_line, this_procedure_notes, this_procedure_notes[-1].Type)
                self.SAS_procedures.append(SAS_procedure)

                this_procedure_notes = []
    
            
    #def ExportToCSV(self, OutputPath, filename):
        pd_output_map = pandas.DataFrame(columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs"])
        
        #
        for i, sasproc in enumerate(self.SAS_procedures):

            if sasproc.ProcType.upper() not in ("LIBREFASSIGN", "LIBREFDEASSIGN") and sasproc.ProcType.upper() !="" :
                data_in_name = list()
                data_out_name = list()
                
                data_in_name = sasproc.data_in
                data_out_name = sasproc.data_out
                
                df = pandas.DataFrame(data=[[str(i), str(sasproc.start_line) ,  str(sasproc.end_line) , sasproc.ProcType.upper(), "|".join(data_in_name), "|".join(data_out_name) ]], \
                                          columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs"])
                pd_output_map = pd_output_map.append(df)
                
        filename = os.path.splitext(os.path.basename(self.path))[0].replace(" ", "_")
        filename_mapping_csv = "mapping_{}.csv".format(filename)
        pd_output_map.reset_index(drop = True, inplace = True)
        #pd_output_map.to_csv(os.path.join(OutputPath, filename_mapping_csv), index=False)
        pd_output_map.to_csv(os.path.join(os.getcwd(), "output", filename_mapping_csv), index=False)

input_path = r"C:\work\IDR\ScotiaGlobe\saslogs"
#sas_logs = get_list_log( os.path.join(os.getcwd(), "source"))
sas_logs = get_list_log(input_path)
for file in sas_logs:
    
    SAS_log = SASLog(file)
    
    #creating graph
    G = nx.MultiDiGraph()
    
    for comp in SAS_log.SAS_procedures:
        try:
            for data_in in comp.data_in:
                #data_name_in = ".".join(data_in)
                data_name_in = data_in
                for data_out in comp.data_out:
                    #data_name_out = ".".join(data_out)
                    data_name_out = data_out
                    G.add_edge(data_name_in, data_name_out, label = comp.ProcType.upper())
            # print(comp.data_out)
        except Exception:
            pass
    
    # print("EDGES")
    # for edge in sorted(G.edges()):
    #     print(edge)
    #
    # print("NODES")
    # for node in sorted(G.nodes()):
    #     print(node)
    
    #convert to directed graph
    DG = G.to_directed()
    DG.graph['graph'] = {'rankdir': 'LR', 'splines': 'line'}
    fname = os.path.splitext(os.path.basename(file))[0].replace(" ", "_")
    nx.drawing.nx_pydot.write_dot(DG, os.path.join("output", 'flow_{}.dot'.format(fname)))
    
    if True:
        print("SAS log processed: "
              "\t {} \n". format(file))

    if False:    
        for note_message in SAS_log.note_messages:
            print ("---------------------\n" + note_message.contents)
        #for macro_gen in SAS_log.macro_gens:
            #print("---------------------\n" + macro_gen.contents)
        #for warning_message in SAS_log.warning_messages:
            #print ("---------------------\n" + warning_message.contents)
        #for script_line in SAS_log.script_lines:
            #print ("---------------------\n" + script_line.contents)
        #for misc_message in SAS_log.misc_messages:
            #print ("---------------------\n" + misc_message.contents)
