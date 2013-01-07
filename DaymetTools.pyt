import arcpy
import os
import sys
import datetime
import collections
import glob
import subprocess
import shutil
#import the cPickle module if available. Other wise use the pure python pickle module.
try:
    import cPickle as pickle
except Exception as ex:
    import pickle

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [CreateDailyRasters]


class CreateDailyRasters(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Daily Rasters"
        self.description = ""
        self.canRunInBackground = False
        #store the display name and corresponding file names for daymet variables
        self.dayMetVariables = collections.OrderedDict((("DAY_LENGTH", "dayl.nc"),
                                ("MAX_TEMPERATURE", "tmax.nc"),
                                ("MIN_TEMPERATURE", "tmin.nc"),
                                ("PRECIPITATION", "prcp.nc"),
                                ("SHORTWAVE_RADIATION", "srad.nc"),
                                ("SNOW_WATER_EQUIVALENT","swe.nc"),
                                ("VAPOR_PRESSURE_DEFICIT","vp.nc")))

    def getParameterInfo(self):
        """Define parameter definitions"""
        #Input Daymet data folder parameter
        param0 = arcpy.Parameter("input_daymet_data_folder", "Input Daymet Data Folder", "Input", "Folder",
                                 "Required")
        
        #Daymet variable Parameter
        param1 = arcpy.Parameter("daymet_variable", "Daymet Variable", "Input", "String", "Required")
        param1.filter.type = "ValueList"
        param1.filter.list = self.dayMetVariables.keys()
        
        #Tiles to Process parameter
        param2 = arcpy.Parameter("tiles_to_process", "Tiles to Process", "Input", "String", "Required", 
                                 multiValue=True)
        param2.filter.type = "ValueList"
        
        #start year parameter
        current_year = datetime.datetime.now().year
        param3 = arcpy.Parameter("start_year", "Start Year", "Input", "Long", "Required")
        param3.filter.type = "Range"
        param3.filter.list = [1980, current_year]
        param3.value = 1980
        
        #end year parameter
        param4 = arcpy.Parameter("end_year", "End Year", "Input", "Long", "Required")
        param4.filter.type = "Range"
        param4.filter.list = [1980, current_year]
        param4.value = current_year - 2
        
        #Output folder parameter
        param5 = arcpy.Parameter("output_folder", "Output Folder", "Input", "Folder", "Required")        
        param5.value = os.path.dirname(arcpy.env.scratchFolder)
        
        #Succeeded derived output parameter
        param6 = arcpy.Parameter("succeeded", "Succeeded", "Output", "Boolean", "Derived")
        
        params = [param0, param1, param2, param3, param4, param5, param6]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        
        #Populate Tiles to process parameter domain based on input daymet data folder.
        daymet_folder_param = parameters[0]
        daymet_folder = daymet_folder_param.valueAsText
        if daymet_folder_param.altered:
            if os.path.exists(daymet_folder):
                parameters[2].filter.list = os.listdir(daymet_folder)
            else:
                parameters[2].filter.list = []
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        
        if parameters[3].value and parameters[4].value:
            start_year = int(parameters[3].valueAsText)
            end_year = int(parameters[4].valueAsText)
            if end_year - start_year < 0:
                parameters[4].setErrorMessage("End year value can not be earlier than the start year value")
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        
        #Get the parameter values
        #daymet_folder = parameters[0].valueAsText
        #daymet_variable = parameters[1].valueAsText
        #tiles_to_process = parameters[2].valueAsText.split(";")
        #start_year = int(parameters[3].valueAsText)
        #end_year = int(parameters[4].valueAsText)
        #output_folder = parameters[5].valueAsText
        
        cwd = os.path.dirname(__file__)
        
        #debug only
        #daymet_folder = os.path.join(os.path.dirname(cwd), "DayMet")
        #daymet_variable = "MAX_TEMPERATURE"
        #tiles_to_process = ["11379", "11380", "11381"]
        #start_year = 1980
        #end_year = 1982
        #output_folder = os.path.join(os.path.dirname(cwd), "toolOutput")        
        
        
        tool_succeeded = False
        daymet_file_name = self.dayMetVariables[daymet_variable]
        daymet_variable_name = os.path.splitext(daymet_file_name)[0]
        #Create an folder within output folder to store intermidiate results
        temp_folder = os.path.join(output_folder, "{0}_temp_rasters".format(daymet_variable_name))
        if os.path.exists(temp_folder):
            shutil.rmtree(temp_folder)
        os.mkdir(temp_folder)
                
        #Get the daymet files to process
        daymet_files = []
        folders = [fld for fld in os.listdir(daymet_folder) if fld in tiles_to_process]
        for fld in folders:
            date_range = [fld + "_" + str(subfld) for subfld in xrange(start_year, end_year + 1)]
            for subfld in date_range:
                file_path = os.path.join(daymet_folder, fld, subfld, daymet_file_name)
                if os.path.exists(file_path):
                    daymet_files.append(file_path)
        
        #Get the path to python exe
        pyexe = os.path.join(sys.exec_prefix, "python.exe")
        worker_py_file = os.path.join(cwd,"worker.py")#pickle the yearly_raster_files list
        pickled_daymet_files = os.path.join(temp_folder, "pickled_daymet_files_list.log")
        with open(pickled_daymet_files, "wb", buffering=0) as pickled_daymet_files_fp:
            pickle.dump(daymet_files, pickled_daymet_files_fp, pickle.HIGHEST_PROTOCOL)
        #Launch the main worker which will process each file on a child process
        pyexe_args = [pyexe, worker_py_file, "process_daymet_file", pickled_daymet_files, temp_folder]
        #Hide the console window that is launched when the python.exe executes in seperate process
        si = subprocess.STARTUPINFO()
        si.dwFlags = subprocess.STARTF_USESHOWWINDOW | subprocess.SW_HIDE        
        messages.addMessage("Processing daymet files")
        try:
            failed_bands = subprocess.check_output(pyexe_args,startupinfo=si)
            if failed_bands:
                failed_bands = eval(failed_bands)
            else:
                failed_bands = []
        except subprocess.CalledProcessError as ex:
            messages.addErrorMessage("An error occured when processing files on remote process.")
            messages.addErrorMessage(ex.output)
            failed_bands = []
            arcpy.SetParameter(6,False)
        
        #Process any failed bands
        if any(failed_bands):
            msg = "Some bands did not process correctly on remote process. Processing them again"
            messages.addMessage(msg)            
            for failed_band in failed_bands:
                if failed_band:
                    for band in failed_band:
                        msg = "Exporting {0}".format(os.path.basename(band[1]))
                        messages.addMessage(msg)
                        arcpy.management.CopyRaster(band[0], band[1])                        
        
        #Mosaic same band from each tile for a given year. This is also done on seperate processes 
        #to speed up tool execution
        #Get a list of all files for a given year
        yearly_raster_files = []
        for year in xrange(start_year, end_year+1):
            yearly_raster_files.append(glob.glob(os.path.join(temp_folder,"*_{0}_Band*.tif".format(year))))
        #pickle the yearly_raster_files list
        pickled_rasters = os.path.join(temp_folder, "pickled_rasters_list.log")
        with open(pickled_rasters, "wb", buffering=0) as pickled_rasters_fp:
            pickle.dump(yearly_raster_files, pickled_rasters_fp, pickle.HIGHEST_PROTOCOL)
        pyexe_args = [pyexe, worker_py_file, "mosaic_rasters", pickled_rasters, output_folder]
        messages.addMessage("Mosaicking daily rasters")
        try:
            failed_mosaics = subprocess.check_output(pyexe_args,startupinfo=si)
            if failed_mosaics:
                failed_mosaics = eval(failed_mosaics)
            else:
                failed_mosaics = []
        except subprocess.CalledProcessError as ex:
            messages.addErrorMessage("An error occured when mosaicking files on remote process.")
            messages.addErrorMessage(ex.output)
            failed_mosaics = []
            arcpy.SetParameter(6,False)
        
        #Process any failed mosaics
        if any(failed_mosaics):
            msg = "Some mosaics did not process correctly on remote process. Processing them again"
            messages.addMessage(msg)            
            for failed_mosaic in failed_mosaics:
                if failed_mosaic:
                    for mosaic in failed_mosaic:
                        msg = "Mosaicking {0} to {1}".format(";".join(map(os.path.basename,failed_mosaic[0])),
                                                             failed_mosaic[1])
                        messages.addMessage(msg)
                        arcpy.management.MosaicToNewRaster(failed_mosaic[0], output_folder, failed_mosaic[1],
                                                           pixel_type="32_BIT_FLOAT",number_of_bands=1)        
        
        #Delete all the intermidiate rasters and log files
        msg = "Deleting all the intermidiate rasters and log files"
        messages.addMessage(msg)
        shutil.rmtree(temp_folder)
        ##Delete any *.log files from output folder
        log_files = glob.glob(os.path.join(output_folder, "*.log"))
        for log_file in log_files:
            try:
                os.remove(log_file)
            except Exception as ex:
                messages.addWarningMessage("Failed to delete {0}".format(log_file))      
                
        arcpy.SetParameter(6,True)
        return
    
#Use only when debugging
#def main():
    #'''Function to debug the tool'''
    #class Messages(object):
        #def addMessage(self, msg):
            #print msg
        #def addErrorMessage(self, msg):
            #print msg
        #def addWarningMessage(self, msg):
            #print msg        
            
    #msgs = Messages()
    #tbx = Toolbox()
    #tool = CreateDailyRasters()
    #tool.execute(tool.getParameterInfo(), msgs)

#if __name__ == "__main__":
    #main()