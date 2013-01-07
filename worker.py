import multiprocessing
import os
import sys
import arcpy
import time
import logging
import fnmatch
#import the cPickle module if available. Other wise use the pure python pickle module.
try:
    import cPickle as pickle
except Exception as ex:
    import pickle


def print_message(msg, stream=sys.stdout):
    '''prints a message to a stream'''
    stream.write(msg)
    if not msg.endswith("\n"):
        stream.write("\n")

def process_daymet_file(daymet_file):
    '''Worker function that process a single daymet file'''
    #Create a logger for each process
    process = multiprocessing.current_process()
    process_log_file = os.path.join(output_folder,"worker_{0}.log".format(process.pid))
    logging.basicConfig(filename=process_log_file,level=logging.INFO,filemode="w", format=log_format)
    
    daymet_file_path_parts = daymet_file.split(os.sep)[-3:]
    
    msg = "Processing {0}".format(os.sep.join(daymet_file_path_parts))
    logging.info(msg)
    
    #Create NetCDF layer
    daymet_variable_name = os.path.splitext(daymet_file_path_parts[-1])[0]
    netcdf_layer = "{0}_{1}".format(daymet_variable_name,daymet_file_path_parts[1])
    msg = "Creating NetCDF layer {0}".format(netcdf_layer)
    logging.info(msg)
    arcpy.md.MakeNetCDFRasterLayer(daymet_file,daymet_variable_name,"x","y",netcdf_layer,"time",
                                   "#","BY_VALUE")
    
    #Copy the NetCDF layer as a TIF file.
    multiband_raster_name = "NetCDF_Raster_{0}.tif".format(netcdf_layer)
    multiband_raster = os.path.join(output_folder, multiband_raster_name)
    msg = "Creating raster {0} from the NetCDF layer".format(multiband_raster_name)
    logging.info(msg)
    arcpy.management.CopyRaster(netcdf_layer, multiband_raster)
    
    #Read number of band information from saved TIF
    band_count = int(arcpy.management.GetRasterProperties(multiband_raster, "BANDCOUNT").getOutput(0))
    failed_bands = []
    #Loop through the bands and copy bands as a seperate TIF file.
    for band in range(1,band_count + 1):
        input_singleband_raster = os.path.join(multiband_raster,"Band_" + str(band))
        output_singleband_raster = os.path.join(output_folder, "{0}_Band_{1}.tif".format(netcdf_layer,band))
        msg = "Exporting {0}".format(os.path.basename(output_singleband_raster))
        logging.info(msg)
        try:
            arcpy.management.CopyRaster(input_singleband_raster, output_singleband_raster)
            time.sleep(0.5)
        except arcpy.ExecuteError as ex:
            for msg in arcpy.GetMessages().split("\n"):
                logging.warning(msg)
            failed_bands.append((input_singleband_raster, output_singleband_raster))

    #Retry exporting failed bands
    skipped_bands = []
    for failed_band in failed_bands:
        msg = "Re-exporting {0}".format(os.path.basename(failed_band[1]))
        logging.info(msg)        
        try:
            arcpy.management.CopyRaster(failed_band[0], failed_band[1])
        except arcpy.ExecuteError as ex:
            skipped_bands.append(failed_band)
            msg = "A GP error occured. The band will be skipped"
            logging.error(msg)
            for msg in arcpy.GetMessages().split("\n"):
                logging.warning(msg)
    #return failed bands that were not processed even with second attempt.
    return skipped_bands

def mosaic_rasters(raster_files):
    '''mosaic rasters for a band from all the tiles to produce a daily raster. '''
    #Create a logger for each process
    process = multiprocessing.current_process()
    process_log_file = os.path.join(output_folder,"worker_{0}.log".format(process.pid))
    logging.basicConfig(filename=process_log_file,level=logging.INFO,filemode="w", format=log_format)
    
    first_file = raster_files[0]
    input_folder = os.path.dirname(first_file)
    file_name_parts = os.path.basename(first_file).split("_")
    daymet_variable_name = file_name_parts[0]
    year = file_name_parts[2]
    raster_file_names = map(os.path.basename, raster_files)
    for band_no in xrange(1,366):
        daily_raster_names = fnmatch.filter(raster_file_names, "*_Band_{0}.tif".format(band_no))
        input_rasters = [os.path.join(input_folder, name) for name in daily_raster_names]
        output_raster_name = "{0}_{1}.tif".format(daymet_variable_name, format_date(band_no) + str(year))
        msg = "Mosaicking {0} to {1}".format(";".join(daily_raster_names),output_raster_name)
        logging.info(msg)
        failed_mosaics = []
        try:
            arcpy.management.MosaicToNewRaster(input_rasters, output_folder, output_raster_name,
                                               pixel_type="32_BIT_FLOAT",number_of_bands=1)
        except arcpy.ExecuteError as ex:
            for msg in arcpy.GetMessages().split("\n"):
                logging.warning(msg)
            failed_mosaics.append((input_rasters,output_raster_name))
    return failed_mosaics

def format_date(day_number):
    '''return a string ddmm given a day in a year. It assumes that every year has only 365 days.
    So even for leap year, the month of february still has 28 days.'''
    
    for i,month_days in enumerate(days_in_month):
        delta = day_number - month_days
        if delta <= 0:
            #when here we have found the month
            month = i + 1
            day = day_number
            break
        else:
            day_number = delta
    date = str(month).zfill(2) + str(day).zfill(2)
    return date

def main(target_function_name, files):
    '''Main function that calls the worker function on multiple cores'''
    
    #Get a list of files to process
    with open(files, "rb") as files_fp:
        files_to_process = pickle.load(files_fp)
        
    #Spawn worker process equal to half the number of cores or equal to number of files to process if less.
    try:
        cpu_count = multiprocessing.cpu_count()
    except NotImplementedError:
        cpu_count = 1
    if cpu_count < 2:
        total_workers = 1
    else:
        #using integer division to get conservative estimates.
        half_cpu = cpu_count / 2
        file_count = len(files_to_process)
        if file_count < half_cpu:
            total_workers = file_count
        else:
            total_workers = half_cpu
    
    #Get the target function based on the command line argument
    this_module = sys.modules[__name__]
    target_func = getattr(this_module, target_function_name)
    
    pool = multiprocessing.Pool(total_workers, maxtasksperchild=1)
    result = pool.map(target_func, files_to_process)
    # Synchronize the main process with the job processes to ensure proper cleanup.
    pool.close()    
    pool.join()
    print_message(str(result))
    
#Some global variables to be used inside the worker function.
output_folder = sys.argv[3]
log_format = "%(levelname)s - %(process)d - %(processName)s - %(asctime)s - %(message)s"
days_in_month = (31,28,31,30,31,30,31,31,30,31,30,31)
if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])