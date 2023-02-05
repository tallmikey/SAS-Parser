#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
SAS Program Parser to Display the Data Workflow Implied

.. pseudocode::

    - Open .sas files
    - Ignore comments
    - Identify Input
    - Identify Output
    - Link input and output together
    - Create a schema to represent the workflow and export the flow to .dot file
    - Export the input/output to csv files

.. warning::

    Assume that program parsed can be run without error

.. DONE:: adjust consistency among extracted components
.. DONE:: identify Macro variable input and output. 
.. todo:: identify PROCs/DATA STEPs run thru multiple lines
.. todo:: identify data flows when macro calls involved.

.. Bugs::
.. Data step output pickup extra data step statements
"""
import os
import shutil
import glob
import re
import operator
import warnings
import networkx as nx
import matplotlib.pyplot as plt
import pandas
from tkinter.tix import COLUMN
from networkx import graph

def get_list(sp_path):
    script_list = list()
    for root, dirs, files in os.walk(sp_path):
        for file in files:
            if file.lower().endswith(".sas"):
                 script_list.append(os.path.join(root, file))
    return script_list

class SASScriptComponent:
    def __init__(self, start, end, content):
        self.start = start
        self.end = end
        self.content = content.strip()
        self.regex_sas_data_name = r"(?:([a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})\.)?" \
                                   r"([a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})"


class Comment(SASScriptComponent):
    def __init__(self, start, end, content):
        super(Comment, self).__init__(start, end, content)


class CommentBlock(Comment):
    def __init__(self, start, end, content):
        super(CommentBlock, self).__init__(start, end, content.group(1))


class CommentInline(Comment):
    def __init__(self, start, end, content):
        super(CommentInline, self).__init__(start, end, content.group(0))
        
        
class DataStep(SASScriptComponent):
    def __init__(self, start, end, content):
        super(DataStep, self).__init__(start, end, content.group(1))
        self.name = "DataStep"
        regex_data = re.compile(r"((?:(?i)^[\s]*data)(?:[\s]+(?:(?:[a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})\.)?"
                                r"(?:[a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})(?:[\s]*\(.*.*?\))?)+;)",
                                re.DOTALL)
        self.data_out = []
        self.data = re.search(regex_data, self.content).group(1)
        regex_data_clean = re.compile(r"(?:(?i)^[\s]*data[\s]+)|"
                                      r"(?:\(.*?(?:\(.*?\).*?)*\))|"
                                      r";", re.DOTALL)
        data_clean = re.sub(regex_data_clean, "", self.data)
        regex_out = re.compile(r"(?:([a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})\.)?"
                               r"([a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})", re.DOTALL)
        for m in re.findall(regex_out, data_clean):
            if m[0] == "" or m[0] is None:
                m = ("work", m[1])
            self.data_out.append(m)

        self.data_in = []
        regex_set = re.compile(r"((?:(?i)[\s]*set)(?:[\s]+" +
                               self.regex_sas_data_name +
                               r"(?:[\s]*\(.*?(?:\(.*?\).*?)*\))?)+;)", re.DOTALL)
        try:
            self.set = re.search(regex_set, self.content).group(0)
        except AttributeError:
            self.set = None
        else:
            regex_set_clean = re.compile(r"(?:(?i)^[\s]*set[\s]+)|"
                                         r"(?:\(.*?(?:\(.*?\).*?)*\))|"
                                         r";", re.DOTALL)
            set_clean = re.sub(regex_set_clean, "", self.set)
            regex_in = re.compile(self.regex_sas_data_name, re.DOTALL)
            
            m = re.match(regex_in, set_clean)
            if m != None:
                if m[0] == "" or m[0] is None:
                    m = ("work", m[1])
                self.data_in.append(m)
            
            """    
            for m in re.findall(regex_in, set_clean):
                if m[0] == "" or m[0] is None:
                    m = ("work", m[1])
                self.data_in.append(m)
                break
            """

class ProcSQL(SASScriptComponent):
    def __init__(self, start, end, content):
        super(ProcSQL, self).__init__(start, end, content.group(1))
        self.name = "ProcSQL"
        self.data_out = []
        regex_out = re.compile(r"(?:(?i)create[\s]+(?:table|view)[\s]+)" +
                               self.regex_sas_data_name +
                               r"(?:(?i)[\s]+as[\s]+)", re.DOTALL)
        #Added if condition by Michael Shi
        if re.search(regex_out, self.content) != None:
            m = re.search(regex_out, self.content).groups()
            if m[0] == "" or m[0] is None:
                m = ("work", m[1])
            self.data_out.append(m)
        
        #insert into
        regex_out = re.compile(r"(?:(?i)insert[\s]+(?:into)[\s]+)" +
                               self.regex_sas_data_name +
                               r"(?:.|\s)*?", re.DOTALL)
        
        if re.search(regex_out, self.content) != None:
            m = re.search(regex_out, self.content).groups()
            if m[0] == "" or m[0] is None:
                m = ("work", m[1])
            self.data_out.append(m)
        
        #update
        regex_out = re.compile(r"(?:(?i)update[\s]+)" +
                               self.regex_sas_data_name +
                               r"(?:.|\s)*?", re.DOTALL)
        
        if re.search(regex_out, self.content) != None:
            m = re.search(regex_out, self.content).groups()
            if m[0] == "" or m[0] is None:
                m = ("work", m[1])
            self.data_out.append(m)
            
        self.data_in = []
        regex_in_main = re.compile(r"(?:(?i)from[\s]+)" +
                                   self.regex_sas_data_name, re.DOTALL)
        if re.search(regex_in_main, self.content) !=None:
            m = re.search(regex_in_main, self.content).groups()
            if m[0] == "" or m[0] is None:
                m = ("work", m[1])
            self.data_in.append(m)

        regex_in_sub = re.compile(r"(?:(?i)(?:inner|(?:left|right|full)?outer)?[\s]+join[\s]+)" +
                                  self.regex_sas_data_name, re.DOTALL)
        for m in re.findall(regex_in_sub, self.content):
            if m[0] == "" or m[0] is None:
                m = ("work", m[1])
            self.data_in.append(m)


class ProcStandard(SASScriptComponent):
    def __init__(self, start, end, content):
        super(ProcStandard, self).__init__(start, end, content.group(1))
        self.name = content.group(2).lower()
        self.data_in = []
        self.data_out = []
        
        if self.name in ["sort"]:
            #Revised by Michael Shi                         
            regex_in_out = re.compile(r"^[\s]*(?:(?i)proc[\s]+sort[\s]+data[\s]*=[\s]*)" +
                                      self.regex_sas_data_name + r"(?:.|\s)*?"
                                      r"(?:(?:(?i)out[\s]*=[\s]*)" +
                                      self.regex_sas_data_name + r"\s+.*)?;",
                                      re.DOTALL)
            m = re.search(regex_in_out, self.content).groups()
            if m[0] == "" or m[0] is None:
                m = ("work", m[1], m[2], m[3])
            if m[2] == "" or m[2] is None:
                m = (m[0], m[1], "work", m[3])
            if m[3] is None:
                m = ("work", m[1], m[2], m[1])
            self.data_in.append((m[0], m[1]))
            self.data_out.append((m[2], m[3]))
            
        elif self.name in ["import"]:
            #Revised by Michael Shi                         
            reg_str = r"^[\s]*(?:(?i)proc[\s]+import[\s]+(?:datafile|datatable)[\s]*=[\s]*)" + \
                r"([a-zA-Z_&'\"][a-zA-Z0-9_&'\"\.:\\\/\-]*)" + r"(?:[^out]+)"    \
                r"(?:(?:(?i)out[\s]*=[\s]*)" +    \
                self.regex_sas_data_name + r")?"
                
            regex_in_out = re.compile(r"^[\s]*(?:(?i)proc[\s]+import[\s]+(?:datafile|datatable)[\s]*=[\s]*)" +
                                      r"([a-zA-Z_&'\"][a-zA-Z0-9_&'\"\.:\\\/\-]*)" + r"(?:[^out]+)"
                                      r"(?:(?:(?i)out[\s]*=[\s]*)" +
                                      self.regex_sas_data_name + r")?",                                  #+ r"\s+.*)?;"
                                      re.DOTALL)
            
            if re.search(regex_in_out, self.content) != None:
                m = re.search(regex_in_out, self.content).groups()
                
                if m[1] == "" or m[1] is None:
                    m = (m[0],  "work", m[2])
                if m[2] is None:
                    m = (m[0], m[1], "none")

                self.data_in.append(("none", m[0]))
                self.data_out.append((m[1], m[2]))
            
        elif self.name in ["export"]:
            #Revised by Michael Shi                         
            reg_str = r"^[\s]*(?:proc[\s]+export[\s]+data[\s]*=[\s]*)" + \
                self.regex_sas_data_name + r"(?:[^out]+)" + \
                r"(?:(?:outfile|outtable)[\s]*=[\s]*)([a-zA-Z_&'\"][a-zA-Z0-9_&\-'\"\.:\\\/]*)\s+.*?"
                
            regex_in_out = re.compile(reg_str, re.DOTALL)
            
            if re.search(regex_in_out, self.content) != None:
                m = re.search(regex_in_out, self.content).groups()
                
                if m[0] == "" or m[0] is None:
                    m =  ( "work", m[1], m[2])
                if m[1] is None:
                    m = (m[0], "none", m[2])

                self.data_in.append(( m[0], m[1]))
                self.data_out.append(("none", m[2]))
                
class MacroCall(SASScriptComponent):
    def __init__(self, start, end, content):
        super(MacroCall, self).__init__(start, end, content.group(1))
        self.name = content.group(2).lower()


class MacroCallUserDef(MacroCall):
    def __init__(self, start, end, content):
        super(MacroCallUserDef, self).__init__(start, end, content)
        self.type = "user_defined"
        self.name = "MacroCall"


class MacroCallSAS(MacroCall):
    def __init__(self, start, end, content):
        super(MacroCallSAS, self).__init__(start, end, content.group(1))
        self.type = "sas_defined"
        self.name = "MacroCall"


class MacroVarLetSAS(SASScriptComponent):
    def __init__(self, start, end, content):
        super(MacroVarLetSAS, self).__init__(start, end, content.group(1))
        self.type = "MacroVarLetSAS"
        #self.name = "MacroVar"
        self.name = content.group(2).lower()
        
        if content.group(2).lower() == "let":
            self.data_out = []
            regex_out = re.compile(r"(?i)^[\s]*(?:%(?:let)[\s]+(?:([a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})[\s]*=[\s]*(.+))[\s]*;).*$", re.DOTALL)
            
            if re.search(regex_out, self.content) != None:
                m = re.search(regex_out, self.content).groups()
                self.data_out.append(m)
        
class MacroInputVarSAS(SASScriptComponent):
    def __init__(self, start, end, content):
        super(MacroInputVarSAS, self).__init__(start, end, content.group(1))
        self.type = "MacroInputVarSAS"
        self.name = "Macro Variables"
        #self.name = content.group(2).lower()
        
        regex_in = re.compile(r"(&[a-zA-Z_][a-zA-Z0-9_]{0,31})")
        self.data_in = []
        
        for m in re.finditer(regex_in, self.content):
            #m = re.search(regex_out, self.content).groups()
            data_in_list = []
            data_in_list.append(m.group(1))
            data_in_list.append(self.content)
            self.data_in.append(data_in_list)
            
            #self.data_in.append(m.groups(1))
            #self.data_in.append(self.content)
        #self.data_in.append(content.group(1))
        """
        regex_out = re.compile(r"(?i)^[\s]*(?:%(?:let)[\s]+(?:([a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})[\s]*=[\s]*(.+))[\s]*;).*$", re.DOTALL)
        
        if re.search(regex_out, self.content) != None:
            m = re.search(regex_out, self.content).groups()
            self.data_out.append(m)
        """
        
class MacroVarSymputSAS(SASScriptComponent):
    def __init__(self, start, end, content):
        super(MacroVarSymputSAS, self).__init__(start, end, content.group(1))
        self.type = "MacroVarSymputSAS"
        #self.name = "MacroVar"
        self.name = content.group(2).lower()
               
        if content.group(2).lower() == "symput":
            regex_out = r"(?i)^[\s]*(?:(?:call[\s]+symput)[\s]*\((?:\'|\")(?:([a-zA-Z_&][a-zA-Z0-9_&\.]{0,31})(?:\'|\")[\s]*,[\s]*(.+))[\s]*\)[\s]*;).*$"
            self.data_out = []
            
            if re.search(regex_out, self.content) != None:
                m = re.search(regex_out, self.content).groups()
                self.data_out.append(m)           
                
class SASProgram:
    
    def __init__(self, path):
        self.path = path
        with open(self.path, "r") as infile:
            self.script = infile.readlines()
            
            """
            # Merge one SAS statement into the same line, LF by ";" Michael Shi
            raw_script = list()
            for line in self.script:
                raw_script.append(line.replace(";",";line_seperator" ).replace("\n", " "))
                raw_script_lines = "".join(raw_script)
            
            self.script = raw_script_lines.split("line_seperator")
            """
            self.script_length = len(self.script)
            
        self.components = []
        regex_comment_block_total = r"^.*(\/\*.*?\*\/).*$"
        regex_comment_block_beg = r"^.*\/\*.*$"
        regex_comment_block_end = r"^.*\*\/.*$"
        self.comment_block = self.find_component(CommentBlock,
                                                 regex_comment_block_total,
                                                 regex_comment_block_beg,
                                                 regex_comment_block_end)
        self.extract(self.comment_block)
        
        """
        Macro Variables
        """
        regex_macro_var_sas_total = r"^(.*(&[a-zA-Z_][a-zA-Z0-9_]{0,31}).*$)"
        regex_macro_var_sas_beg = r"(?!.*)"
        regex_macro_var_sas_end = r"(?!.*)"
        self.macro_invar_sas = self.find_component(MacroInputVarSAS,
                                                  regex_macro_var_sas_total,
                                                  regex_macro_var_sas_beg,
                                                  regex_macro_var_sas_end)
        
        
        """
        Macro %let statement
        """
        regex_macro_var_sas_total = r"^[\s]*(%((?i)let).*;).*$"
        regex_macro_var_sas_beg = r"^[\s]*%(?:(?i)let).*$"
        regex_macro_var_sas_end = r"^.*;.*$"
        self.macro_var_let_sas = self.find_component(MacroVarLetSAS,
                                                  regex_macro_var_sas_total,
                                                  regex_macro_var_sas_beg,
                                                  regex_macro_var_sas_end)
        self.extract(self.macro_var_let_sas)
        
        """
        Macro call symput statement
        """
        regex_macro_var_sas_total = r"(?i)^[\s]*(call[\s]+(symput).*;).*$"
        regex_macro_var_sas_beg = r"(?i)^[\s]*(call[\s]+(symput)).*"
        regex_macro_var_sas_end = r"^.*;.*$"
        self.macro_var_symput_sas = self.find_component(MacroVarSymputSAS,
                                                  regex_macro_var_sas_total,
                                                  regex_macro_var_sas_beg,
                                                  regex_macro_var_sas_end)
        self.extract(self.macro_var_symput_sas)
        
        """
        Data Step
        """
        regex_data_step_total = r"^[ ]*((?:(?i)data)[\s]+.*?;(?:.*?;)*?[\s]*(?:(?i)run);).*$"
        regex_data_step_beg = r"^[ ]*(?:(?i)data)[\s]+.*$"
        regex_data_step_end = r"^[ ]*(?:(?i)run);.*$"
        self.data_step = self.find_component(DataStep,
                                             regex_data_step_total,
                                             regex_data_step_beg,
                                             regex_data_step_end)
        self.extract(self.data_step)
        
        """
        PROC SQL
        """
        regex_proc_sql_total = r"^[ ]*((?:(?i)proc[\s]+sql)(?:[\s]+.*?)?;" \
                               r"(?:.*?;)*?[\s]*" \
                               r"(?:(?i)run[\s]*;|quit[\s]*;|proc[\s]*)).*$"          #r"(?:(?i)run|quit);).*$"
                               
        regex_proc_sql_beg = r"^[ ]*((?i)proc[\s]+sql)(?:[\s]+.*?)?;.*$"
        regex_proc_sql_end = r"^[ ]*((?i)run[\s]*;|quit[\s]*;|proc[\s]*).*$"       #r"^[ ]*(?:(?i)run|quit);.*$"
        self.proc_sql = self.find_component(ProcSQL,
                                            regex_proc_sql_total,
                                            regex_proc_sql_beg,
                                            regex_proc_sql_end)
        self.extract(self.proc_sql)
        
        """
        #PROC IMPORT, PROC SORT
        """
        regex_proc_std_total = r"^[ ]*((?:(?i)proc[\s]+(sort|import)[\s]+" \
                               r"(?:data|datafile))[\s]*=" \
                               r"(?:.+?);(?:.*?;)*?[\s]*(?:(?i)run);).*$"
        regex_proc_std_beg = r"^[ ]*(?:(?i)proc[\s]+(?:sort|import)[\s]+" \
                             r"(?:data|datafile))[\s]*=(?:.+?)$"
        regex_proc_std_end = r"^[ ]*(?:(?i)run);.*$"
        self.proc_std = self.find_component(ProcStandard,
                                            regex_proc_std_total,
                                            regex_proc_std_beg,
                                            regex_proc_std_end)
        self.extract(self.proc_std)
        
        """
        #PROC EXPORT
        """
        regex_proc_std_total = r"^[ ]*((?:(?i)proc[\s]+(export)[\s]+" \
                               r"(?:data))[\s]*=" \
                               r"(?:.+?);(?:.*?;)*?[\s]*(?:(?i)run);).*$"
        regex_proc_std_beg = r"^[ ]*(?:(?i)proc[\s]+(export)[\s]+" \
                             r"(?:data))[\s]*=(?:.+?)$"
        regex_proc_std_end = r"^[ ]*(?:(?i)run);.*$"
        self.proc_std = self.find_component(ProcStandard,
                                            regex_proc_std_total,
                                            regex_proc_std_beg,
                                            regex_proc_std_end)
        self.extract(self.proc_std)
        
        """ 
        Macro include
        """
        regex_macro_call_user_def_total = r"^[\s]*(%((?i)libname|exist_file)\(.*\);).*$"
        regex_macro_call_user_def_beg = r"^[\s]*%(?:(?i)libname)\(.*$"
        regex_macro_call_user_def_end = r"^.*\);.*$"
        self.macro_call_user_def = self.find_component(MacroCallUserDef,
                                                       regex_macro_call_user_def_total,
                                                       regex_macro_call_user_def_beg,
                                                       regex_macro_call_user_def_end)
        self.extract(self.macro_call_user_def)
        
        
        regex_comment_inline_total = r"^[^=]*(\*+[^;]*;).*$"
        regex_comment_inline_beg = r"^[^=]*\*.*?$"
        regex_comment_inline_end = r"^.*;.*$"
        self.comment_inline = self.find_component(CommentInline,
                                                  regex_comment_inline_total,
                                                  regex_comment_inline_beg,
                                                  regex_comment_inline_end)
        self.extract(self.comment_inline)
        
        filename = os.path.splitext(os.path.basename(self.path))[0].replace(" ", "_")
        filename_residuals = "residuals_{}.txt".format(filename)
        with open(os.path.join(os.getcwd(), "output", filename_residuals), "w") as outfile:
            nb_line_extracted = 0
            for line in self.script:
                if line == "" or line == "\n":
                    line = "\n"
                    nb_line_extracted += 1
                outfile.write(line)
        
        #Output mapping to csv
        pd_output_map = pandas.DataFrame(columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs"])
        mapping = sorted(self.components, key=lambda x: x.start)
        
        for i, step in enumerate(mapping):
            if type(step) in (CommentBlock, CommentInline, Comment, MacroCall):
                continue
            
            if type(step) in (ProcStandard, ProcSQL, DataStep):
                data_in_name = list()
                data_out_name = list()
                
                for x in step.data_in:
                    data_in_name.append(str(x[0]) + "." + x[1])
                
                for x in step.data_out:
                    data_out_name.append(str(x[0]) + "." + x[1])
                
                df = pandas.DataFrame(data=[[str(i), str(step.start) ,  str(step.end) , step.name.upper(), "|".join(data_in_name), "|".join(data_out_name) ]], \
                                          columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs"])
                pd_output_map = pd_output_map.append(df)
            #else:
                #csv_row = csv_row + "," + "," + "\n"
             
        filename_mapping_csv = "mapping_{}.csv".format(filename)
        pd_output_map.reset_index(drop = True, inplace = True)
        pd_output_map.to_csv(os.path.join(os.getcwd(), "output", filename_mapping_csv), index=False)
        
        #Output macro vars to csv
        pd_output_macro = pandas.DataFrame(columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs","Values"])
        macro_var_sas = list()
        for x in (self.macro_var_let_sas):
            macro_var_sas.append(x)
            
        for x in (self.macro_var_symput_sas):
            macro_var_sas.append(x)
        
        for x in (self.macro_invar_sas):
            macro_var_sas.append(x) 
       
        mapping = sorted(macro_var_sas, key=lambda x: x.start)
        
        for i, step in enumerate(mapping):
            
            #csv_row_1 = str(i) + "," + str(step.start) + "," + str(step.end) + "," + step.name.upper() + "," #str(type(step)).split(".")[1].split("'")[0] + "," 
            if type(step) == MacroVarLetSAS:
                
                if step.name.upper() == "LET" and len(step.data_out) > 0:
                    df = pandas.DataFrame(data=[[str(i), str(step.start) ,  str(step.end) , step.name.upper(), "", str(step.data_out[0][0]) , str(step.data_out[0][1])]], \
                                          columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs","Values"])
                    pd_output_macro = pd_output_macro.append(df)
                    #csv_row = csv_row_1 + "," + str(step.data_out[0][0]) + "," + str(step.data_out[0][1]) + "\n" 
                     
                
            elif  type(step) == MacroVarSymputSAS and len(step.data_out) > 0:
                if step.name.upper() == "SYMPUT":
                    df = pandas.DataFrame(data=[[str(i), str(step.start) ,  str(step.end) , step.name.upper(), "", str(step.data_out[0][0]) , str(step.data_out[0][1]) ]], \
                                          columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs","Values"])
                    pd_output_macro = pd_output_macro.append(df)
                    #csv_row = csv_row_1 + "," + str(step.data_out[0][0]) + "," + str(step.data_out[0][1]) + "\n" 
                    
            elif type(step) == MacroInputVarSAS:
                for data_in_i in step.data_in:
                    df = pandas.DataFrame(data=[[str(i), str(step.start) ,  str(step.end) , step.name.upper(), str(data_in_i[0]) ,"", str(data_in_i[1])]], \
                                          columns=["Sequence","Start Line Number", "End Line Number", "Procedure Type", "Inputs", "Outputs","Values"])
                    pd_output_macro = pd_output_macro.append(df)
                    #csv_row = csv_row_1  + str(data_in_i[0]) + ",," + str(data_in_i[1]) + "\n" 
                    
        pd_output_macro.reset_index(drop = True, inplace = True)
        filename_mapping_csv = "macros_{}.csv".format(filename)
        pd_output_macro.to_csv(os.path.join(os.getcwd(), "output", filename_mapping_csv), index=False)
        
                        
        self.prop_extracted = nb_line_extracted/self.script_length
        filename_log = "summary_{}.txt".format(filename)
        with open(os.path.join(os.getcwd(), "output", filename_log), "w") as outfile:
            outfile.write("Number of lines of code in the script: \n" 
              "\t {} \n". format(len(self.script)))
            outfile.write("Proportion of the script correctly extracted: \n"
              "\t {} \n".format("%.3f" % self.prop_extracted))
            outfile.write("Code Diagnostic:")
            outfile.write("\tProportion of the comments: \n"
              "\t\t {} \n".format("%.3f" % self.proportion_comments()))
            outfile.write(self.extraction_summary())
            outfile.write("See the following file for more details on the ignored content: \n"
                          
              " residuals_{}.txt".format(filename))
        print("Number of lines of code in the script: \n"
              "\t {} \n". format(len(self.script)))
        print("Proportion of the script correctly extracted: \n"
              "\t {} \n".format("%.3f" % self.prop_extracted))
        print("Code Diagnostic:")
        print("\tProportion of the comments: \n"
              "\t\t {} \n".format("%.3f" % self.proportion_comments()))
        print(self.extraction_summary())
        print("See the following file for more details on the ignored content: \n"
              " residuals_{}.txt".format(filename))
        # for comp in self.components:
        #     print(comp)
        #     print(comp.start)

    def extract(self, extracted_components):
        assert(isinstance(extracted_components, list)), \
            invalid_type_message.format("extracted_components", "list", type(extracted_components))
        for comp in extracted_components:
            current = self.script[comp.start:comp.end]
            nb_line_current = len(current)
            current = "".join(current).strip()
            #print("CURRENT")
            #print(current)
            modified = current.replace(comp.content, "")
            #print("CONTENT")
            #print(comp.content)
            #print("MODIFIED")
            #print(modified)
            nb_line_modified = len(modified.split("\n"))
            modified = modified + "\n"*(nb_line_current-nb_line_modified)
            #print(modified)
            modified = modified.split("\n")
            #print(modified)
            self.script[comp.start:comp.end] = modified

    def find_component(self, cls, regex_total, regex_beg, regex_end):
        regex_total = re.compile(regex_total, re.DOTALL)
        regex_beg = re.compile(regex_beg)
        regex_end = re.compile(regex_end)
        components = []
        start = None
        end = None
        
        line_stamp = 0
        while line_stamp < len(self.script):
        #for line_stamp in range(len(self.script)):
            line = self.script[line_stamp]
            if re.match(regex_total, line) and start is None:
                
                content = re.search(regex_total, line)
                comp = cls(line_stamp, line_stamp + 1, content)
                components.append(comp)
                """
                obj = re.finditer(regex_total, line)
                
                for m in re.finditer(regex_total, line):
                    comp = cls(line_stamp, line_stamp + 1, m)
                    components.append(comp)
                """
            else:
                if re.match(regex_beg, line):
                    if start is None:
                        start = line_stamp
                    if end is not None:
                        if start >= end:
                            end = None
                if re.match(regex_end, line):
                    if line_stamp == start:
                        # start and end are on the same line, protentially same PROC
                        if  re.findall(r"(?i)^[ ]*(proc[\s]*)", line) != None:
                            m = re.findall(r"(?i)^[ ]*(proc[\s]*)", line)
                            if len(m) > 1:
                                end = line_stamp + 1
                    else:
                        end = line_stamp + 1
                        
                        if re.search(r"(?i)^[ ]*(proc[\s]*)", line) != None and start != None and end != None:
                            #backtrack one line if ends by proc
                            line_stamp = line_stamp - 1

                if start is not None and end is not None:
                    block = "".join(self.script[start:end])
                    content = re.search(regex_total, block)
                    comp = cls(start, end, content)
                    components.append(comp)
                    self.components.append(comp)
                    start = None
                    end = None
                
            line_stamp = line_stamp + 1
            #print(line_stamp)
        return components

    def proportion_comments(self):
        nb_line_comments = 0
        for comment in self.comment_block:
            nb_line_comments += comment.end-comment.start
        for comment in self.comment_inline:
            nb_line_comments += comment.end - comment.start
        return nb_line_comments/self.script_length

    def extraction_summary(self):
        extraction_dict = {
            'comment_block': len(self.comment_block),
            'comment_inline': len(self.comment_inline),
            'data_step': len(self.data_step),
            'proc_sql': len(self.proc_sql),
            'proc_sort': len([x for x in self.proc_std if x.name == 'sort']),
            'proc_import': len([x for x in self.proc_std if x.name == 'import']),
            # 'sas_macro': len(self.macro_call_sas),
            #'include': len([x for x in self.macro_var_sas if x.name == 'include']),
            'let': len([x for x in self.macro_var_let_sas if x.name == 'let']),
            'Symput': len([x for x in self.macro_var_symput_sas if x.name == 'symput']),
            'Macro Variables': len(self.macro_invar_sas)
            #'put': len([x for x in self.macro_call_sas if x.name == 'put']),
            # 'user_def_macro': len(self.macro_call_user_def)
        }
        for macro in set([x for x in self.macro_call_user_def]):
            name = macro.name
            extraction_dict[name] = len([x for x in self.macro_call_user_def if x.name == name])
        text_to_print = "The extraction can be resumed as follow: \n"
        for category, qte in sorted(extraction_dict.items(), key=operator.itemgetter(1), reverse=True):
            text_to_print += "\t{}: {}\n".format(category, qte)
        return text_to_print

#output path
output_path = os.path.join(os.getcwd(), "output")

if os.path.isdir(output_path) == False:
    #shutil.rmtree(output_path)
    os.mkdir(output_path)
    
path = os.path.join(os.getcwd())
#print(path)

#sas_files = glob.glob(os.path.join("**", "*.sas")) #, recursive=True
sas_files = get_list( os.path.join(os.getcwd(), "source"))
#sas_files = get_list(r"C:\work\SAS Code from Balwinder\Shamela_Production_Reports")
#sas_files = get_list(r"C:\work\IDR\ScotiaGlobe")
#print(sas_files)

invalid_type_message = "The argument {} must of the following type: \n\t {} \n" \
                       "The type provided was: \n\t {}"

for file in sas_files:
    G = nx.MultiDiGraph()
    sas = SASProgram(file)
    
    for comp in sas.components:
        try:
            for data_in in comp.data_in:
                data_name_in = ".".join(data_in)
                for data_out in comp.data_out:
                    data_name_out = ".".join(data_out)
                    G.add_edge(data_name_in, data_name_out, label = comp.name)
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

    UG = G.to_undirected()
    sub_graphs = nx.connected_component_subgraphs(UG)
    # sub_graphs = nx.weakly_connected_components(G)

    DG = G.to_directed()
    DG.graph['graph'] = {'rankdir': 'LR', 'splines': 'line'}
    fname = os.path.splitext(os.path.basename(file))[0].replace(" ", "_")
    nx.drawing.nx_pydot.write_dot(DG, os.path.join("output", 'flow_{}.dot'.format(fname)))
    
    try:
        pos=nx.graphviz_layout(DG, prog='dot')
    except:
        pos=nx.spring_layout(DG,iterations=20)
        
    nx.draw(DG)
    plt.savefig("C:\work\SAS-Mapper\output\draw.png")
    pos = nx.drawing.nx_pydot.graphviz_layout(DG, prog='dot')
    nx.draw(DG, pos, with_labels=True, arrow=True)
    p = nx.drawing.nx_pydot.to_pydot(DG)
    p.write_png(os.path.join("output", 'example_{}.png'.format(i)))
    
    #drawing the graph
    #H = nx.graph(G)
    H = G.to_directed()
    edgelabels = []
    
    for (u,v,d) in H.edges(data=True):
        r = d["label"]
        edgelabels.append(r)
    
    try:
        pos=nx.graphviz_layout(H)
    except:
        pos=nx.spring_layout(H,iterations=20)
        
    
    plt.rcParams['text.usetex'] = False
    #plt.figure(figsize=(8,8))
    #nx.draw_networkx_edges(H,pos,alpha=0.3,width=edgewidth, edge_color='m')
    #nodesize=[wins[v]*50 for v in H]
    nx.draw_networkx_nodes(H,pos,alpha=0.4)
    nx.draw_networkx_edges(H,pos,width=1, arrows=True, label= edgelabels )   
    
    font = {'fontname'   : 'Helvetica',
            'color'      : 'k',
            'fontweight' : 'bold',
            'fontsize'   : 14}
    plt.title("Data flow", font)

    # change font and write text (using data coordinates)
    font = {'fontname'   : 'Helvetica',
    'color'      : 'r',
    'fontweight' : 'normal',
    'fontsize'   : 10}

    plt.text(0.5, 0.97, "edge label = SAS DATA STEPs or Procedures used",
             horizontalalignment='center',
             transform=plt.gca().transAxes)
    plt.text(0.5, 0.94,  "node = DATASETs or DATA Tables",
             horizontalalignment='center',
             transform=plt.gca().transAxes)

    plt.axis('off')
    plt.savefig("data_flow.png",dpi=75)
    print("Wrote data_flow.png")
    plt.show() # display
    
    """
    for i, sg in enumerate(sub_graphs):
        # print("subgraph {} has {} nodes".format(i, sg.number_of_nodes()))
        # print("\tNodes:", sg.nodes(data=True))
        # print("\tEdges:", sg.edges())
        # print(sg)
        TG = nx.MultiDiGraph()
        for edge in [edge for edge in G.edges()
                     if edge[0] in sg.nodes()
                     or edge[1] in sg.nodes()]:
            TG.add_edge(*edge)
        TG.graph['graph'] = {'rankdir': 'LR', 'splines': 'line'}
        fname_i = os.path.join("output", "flow_{}.dot".format(fname) + "_{}.dot".format(i+1))
        nx.drawing.nx_pydot.write_dot(TG, fname_i)
        
        nx.draw(TG)
        plt.savefig("C:\work\SAS-Mapper\output\draw.png")
        pos = nx.drawing.nx_pydot.graphviz_layout(TG, prog='dot')
        nx.draw(TG, pos, with_labels=True, arrow=False)
        p = nx.drawing.nx_pydot.to_pydot(TG)
        p.write_png(os.path.join("output", 'example_{}.png'.format(i)))
        """
    # print(sas.comment_block)
    # for cb in sas.comment_block:
    #     print(cb.content)
    #print(len(sas.proc_sql))
    #  for comment in sas.comment_block:
    #      print(comment.content)
    #      print(comment.start)
    #      print(comment.end)
    #      print(sas.script[comment.start:comment.end])
    #for data in sas.data_step:
    #    print(data.content)
    #for proc in sas.proc_sql:
    #    print(proc.content)
    #for proc in sas.proc_std:
    #    print(proc.category)
    # for comment in sas.comment_inline:
    #     print(comment.content)
    # for cb in sas.comment_block:
    #     print(cb.content)
    #     print(cb.end-cb.start)
    #for element in sas.proc_sql:
    #    print(element.content)
