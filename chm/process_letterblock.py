# Will Burt 2023
# Copyright 2023 BC Provincial Government

'''
Usage python -m process_letterblock 082K

This script searches a BCLidar index for maptiles matching the first input parameter
eg. 082k --> like '082k%'

PDAL is used to create DSM using non-ground classified first returns
DEM is from the hosted DEM
CHM is produced by a simple subtraction of DSM - DEM using GDAL

ouput chm are COG TIF in epsg 3857 
DSM is deleted

This is a work in progress
'''

import sys
import os
import requests
import json
from osgeo import ogr
import logging
import shutil
from multiprocessing import Process, Queue,Pool,cpu_count
import subprocess as sp
from math import ceil,floor
from time import time
import tempfile
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#letter_block = '082K016'#sys.argv[1]
root_path = '//objectstore.nrs.bcgov\\gdwuts'
out_path = 'T:/lidar_temp'

def get_lidar_files(letter_block, index='json',product = 'point cloud'):
    ''''Get lidar files in letterblock from index rest service
    index='json' for https json index
    index='file' for local file index
    product = 'point cloud' for point cloud index 
    OR 'dem20k' for 1:20,000 dem tif file index OR 'dem2500' for 1:2,500 Grid DEM
    return dictionary of files with key year
    '''
    assert product in ['point cloud','dem20k','dem2500'], f'Invalid parameter product value of ({product})'
    file_dic =  {}    
    url_root = 'https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services/LiDAR_BC_S3_Public/FeatureServer/'
    json_lookup = {'point cloud':f'{url_root}'+'3/query?',
                    'dem20k':f'{url_root}'+'5/query?',
                    'dem2500':f'{url_root}'+'4/query?'}
    file_lookup = {'point cloud':f'{root_path}'+'/index/update_2023_02-22/Lidar_pointcloud_index_20230222.shp',
                    'dem20k':f'{root_path}'+'/index/update_2023_02-22/Lidar_dem_index_20230222.shp',
                    'dem2500':f'{root_path}'+'/index/update_2023_02-22/Lidar_dem_index_20230222.shp'}
    if index =='json':
        # do the json thing
        index_url = json_lookup[product]
        # perpare request params
        params = {
            'where':f"maptile LIKE '{letter_block}%'",
            'outFields': 'maptile,filename,path,s3Url',
            'f':"json",
            "returnGeometry":False
        }
        r = requests.get(index_url,params=params)
        if not r.ok:
            logging.error(f"Request for json failed: {r.text}")
        else:
            r_json = r.json()
            # list files from json
            file_attributes = [f['attributes'] for f in r_json['features']]
            year = file_attributes['year']
            if year not in file_dic.keys():
                file_attributes[year] = []
            file_dic[year].append(file_attributes)
    elif index == 'file':
        # do the file based index thing
        file_path = file_lookup[product]
        assert os.path.exists(file_path)
        source = ogr.Open(file_path,0)
        lyr = source.GetLayer()
        lyr.SetAttributeFilter(f"maptile LIKE '{letter_block.lower()}%'")
        for feat in lyr:
            fjson = feat.ExportToJson(as_object=True)
            year = fjson['properties']['year']
            if year not in file_dic.keys():
                file_dic[year] = []
            file_dic[year].append(fjson['properties'])

    return file_dic

def create_dsm(las_input,tif_output):
    ''' Create dsm (tif_output) from las_input
        * ll corner will be rounded up to the nearest metre
        * uses chm_pipeline.json

    '''

    pipeline = 'chm_pipeline.json'
    # get las file metadata
    p = sp.run(['pdal','info','--metadata',las_input],capture_output=True)

    # get bounds of lidar file
    info_json = json.loads(p.stdout.decode('UTF-8'))
    metadata = info_json.get("metadata")
    minx = ceil(metadata.get("minx"))
    miny = ceil(metadata.get("miny"))
    maxx = floor(metadata.get("maxx"))
    maxy = floor(metadata.get("maxy"))
    bounds = ([minx,maxx],[miny,maxy])
    
    reader = f'--readers.las.filename={las_input}'
    writer = f'--writers.gdal.filename={tif_output}'
    bounds = f'--writers.gdal.bounds={bounds}'
    if os.path.exists(pipeline) and os.path.exists(las_input):
        p = sp.run(['pdal','pipeline',pipeline, reader, writer,bounds],capture_output=True)
        #print (p.stdout)
    return tif_output

def dsm_by_merge(las_file_list,tif_output):
    ''' dsm_by_merge'''
    logger.info('Starting dsm_by_merge')
    pipeline = 'chm_pipeline_with_merge.json'
    input_file_string = ','.join([f'"{f}"'for f in las_file_list])
    x_coordinates  = []
    y_coordinates = []
    # load pipeline template
    with open(pipeline,'r') as f:
        pipeline_json = json.load(f)
    for f in las_file_list:
        pipeline_json.insert(0,f)
        x,y = get_las_bounds(f)
        x_coordinates = x_coordinates + x
        y_coordinates = y_coordinates + y
    bounds = ([min(x_coordinates),max(x_coordinates)],[min(y_coordinates),max(y_coordinates)])
    logger.debug(f"Bounds for all las files: {bounds}")
    # write updated pipeline    
    f_pipeline = tempfile.NamedTemporaryFile().name + '.json'
    with open(f_pipeline,'w') as f:
        json.dump(pipeline_json,f)
    
    #pipeline overide
    writer = f'--writers.gdal.filename={tif_output}'
    write_bounds = f'--writers.gdal.bounds={bounds}'
    logger.info(f"Starting pdal pipeline with {f_pipeline}")
    p = sp.run(['pdal','pipeline',f_pipeline, writer,write_bounds],capture_output=True)
    os.remove(f_pipeline)
    logger.info("done")
    return tif_output
def create_chm_by_letterblock(letter_block,index='json'):
    ''' This is a multiprocessing daemon for processing letterblocks in a mapsheet
        returns a list of dictionaries {year:[file.tif,file2.tif]}'''
    
    logger.info(f"Creating tasks for {letter_block}")
    cpu = cpu_count()
    if cpu > 4:
        pool = Pool(cpu-4)
    else:
        pool = Pool(1)
    lidar_tiles = get_lidar_files(letter_block=letter_block,index=index,product='point cloud')

    
    mapsheets = []
    for tileset in lidar_tiles.values():
        mapsheets = mapsheets + [t['maptile'].split('_')[0] for t in tileset]
    mapsheets = list(set(mapsheets))
    tasks = []
    logger.info(f"\t tasks:{len(mapsheets)}\tthread pool:{cpu-4}")
    for sheet in mapsheets:
        tasks.append((sheet,'merged',index))
    results = pool.starmap(create_chm_mosaic,tasks)
    r_summary = {}
    files = []

    for result in results:
        for year in result:
            if year not in r_summary.keys():
                r_summary[year] = 0
            r_summary[year] = r_summary[year] + len(result[year])
            files = files + [os.path.basename(f) for f in result[year]]
    logger.info(f"\tprocessing summary: {r_summary}")
    logger.info(f"\tprocessed files: {','.join(files)}")
    # results is a list of dictionaries {year:[file.tif,file2.tif]}
    return results    


def create_chm_mosaic(mapsheet,method='merged',index='json'):
    ''' Builds a chm mosaic from an inputed letterbloack mapsheet like '082K' pr '082K067'
        build chm mosaic following one of two methods
        method=None calculates tiles for letter block. This method may have errors on cutlines
        method='merged'(default) merges the las files from the letterblock for each year availiable
            and calculates the chm from that
        index='json' uses the json index from the open lidar portal
        index='file' uses the index from local files
    '''
    logger.info(f"Starting CHM mosaic process for letter_block={mapsheet}")
    start_time = time()
    lidar_tiles = get_lidar_files(letter_block=mapsheet,index=index,product='point cloud')
    
    chm_list = []

    arg_list = {}
    lidar_file_names = []
    # create chm for tiles by by year
    dsm_rasters = {}
    for year, tiles in lidar_tiles.items():
        arg_list[year] = []
        for tile in tiles:
            las_input = os.path.join(root_path,tile['path'],tile['filename'])
            output = os.path.join(out_path, os.path.splitext(tile['filename'])[0] + f'_{year}.tif')
            if method != 'merged':
                arg_list[year].append((las_input,output))
            lidar_file_names.append(las_input)
        if method == 'merged':
            output = os.path.join(out_path, mapsheet) + f'_{year}_dsm.tif'
            if not os.path.exists(output):
                arg_list[year].append((lidar_file_names,output))
                # dsm_rasters = [dsm_by_merge(lidar_file_names,output)]
            else:
                if year in dsm_rasters.keys():
                    dsm_rasters[year].append(output)
                else:
                    dsm_rasters[year]= [output]
        
    if method == 'merged':
        for year in arg_list.keys():
            if year in dsm_rasters.keys():
                dsm_results = []
                for arg in arg_list[year]:
                    input, dsm_output = arg
                    dsm_results.append(dsm_by_merge(input,dsm_output))
                dsm_rasters[year] = dsm_rasters[year] + dsm_results
            else:
                dsm_results = []
                for arg in arg_list[year]:
                    input, dsm_output = arg
                    dsm_results.append(dsm_by_merge(input,dsm_output))
                dsm_rasters[year] = dsm_results
    else:
        for year in arg_list.keys():
            # result = pool.starmap(create_dsm,arg_list[year])
            result = []
            for arg in arg_list[year]:
                input, dsm_output = arg
                result.append(dsm_by_merge(input,dsm_output)) 
            if year in dsm_rasters.keys():
                dsm_rasters[year] = dsm_rasters[year] + result
            else:
                dsm_rasters[year] = result

    # raster dsm created for each year
    dem_tiles = get_lidar_files(letter_block=mapsheet,index=index,product='dem20k')
    
    # process chm
    arg_list = []
    outfile = {}
    for year in dsm_rasters.keys():
        logger.debug(f'Calculating chm for year: {year}')
        if method != 'merged':
            chm_rasters =[]
            for dsm in dsm_rasters[year]:
                output = os.path.join(out_path,os.path.splitext(os.path.basename(dsm))[0] + '_chm.tif')
                dem = [os.path.join(root_path,d['path'],d['filename']) \
                    for d in dem_tiles[year] if os.path.basename(dsm).split('_')[1] in d['filename']][0]
                arg_list.append((dsm,dem,output))
                chm_rasters.append(create_chm(dsm,dem,output))
            
            outfile[year] = merge_rasters(chm_rasters,out_file= os.path.join(out_path,f'{mapsheet}_chm_cog.tif'))
        else:
            if len(dem_tiles[year])>0:
                output = dsm_rasters[year][0].replace('_dsm.tif','_chm.tif')
                outfile[year] = []
                for dtile in dem_tiles[year]:
                    dem = os.path.join(root_path,dem_tiles[year][0]['path'],dtile['filename'])
                    outfile[year].append(create_chm(dsm_rasters[year][0],dem,output))
            else:
                output = dsm_rasters[year][0].replace('_dsm.tif','_chm.tif')
                dem = os.path.join(root_path,dem_tiles[year][0]['path'],dem_tiles[year][0]['filename'])
                outfile[year] = create_chm(dsm_rasters[year][0],dem,output)

    run_time = round((time() - start_time)/60,1)
    # clean up
    for dsm_set in dsm_rasters.values():
        for dsm in dsm_set:
            logger.debug(f"Removing dsm file: {dsm}")
            os.remove(dsm)
    logger.info(f"Done CHM mosaic process: {run_time} minutes")
    return outfile
    
def create_chm(input_dsm,input_dem,out_tif):
    # gdal_calc -A T:/dsm.tif -B T:/dem.tif --calc="A-B" --extent intersect  --outfile T:/chm.tif
    logger.info(f"Start CHM calc and COG creation: --> {out_tif}")
    file_name = os.path.basename(out_tif)
    tif_tempfile = os.path.join(tempfile.gettempdir(),"temp_" + os.path.splitext(file_name)[0] + '.tif')
    cpu = cpu_count() - 4
    p = sp.run(['gdal_calc','-A',input_dsm, '-B', input_dem, '--calc="round(where((A-B)>0,(A-B),0),2)"',
    '--extent', 'intersect','--outfile', tif_tempfile,'--NoDataValue','-9999', 
    '--type','Float32'],capture_output=True,shell=True)
    # compress and create COG
    if not p.returncode == 0:
        logger.error(p.stdout,p.stderr)
    # gdal_translate 082K006_chm.tif 082K006_chm_cog.tif -of COG -co COMPRESS=LZW -co NUM_THREADS=ALL_CPUS -a_nodata -9999    print (p.stdout)
    p = sp.run(['gdal_translate',tif_tempfile,out_tif,'-of','COG',
    '-co', 'TILING_SCHEME=GoogleMapsCompatible',
    '-co','COMPRESS=DEFLATE',
    '-co',f'NUM_THREADS={cpu}','-a_nodata','-9999'],capture_output=True,shell=True)
    if not p.returncode == 0:
        logger.error(p.stdout,p.stderr)
    logger.info("Done CHM calc and COG creation")
    if os.path.exists(tif_tempfile):
        os.remove(tif_tempfile)
    return out_tif

def merge_rasters(raster_list,out_file,input_text_file=None):
    # gdal_merge -o T:/test_merge.tif T:/raster1.tif T:/raster2.tif
    out_list_file = os.path.join(os.path.dirname(out_file),'merge_list.txt')
    out_vrt = os.path.splitext(out_file)[0] + '.vrt'
    cpu = cpu_count() - 4
    if input_text_file is None:
        rasters = ' '.join(raster_list)
        with open(out_list_file,'w') as f:
            for raster in raster_list:
                f.write(f"{raster}\n")
    else:
        assert os.path.exists(input_text_file)
        out_list_file = input_text_file
    
    #p = sp.run(['gdal_merge','-o',out_file,rasters],capture_output=True,shell=True)
    p = sp.run(['gdalbuildvrt', '-input_file_list', out_list_file, out_vrt,'-vrtnodata','-9999'],capture_output=True,shell=True)
    p = sp.run(['gdal_translate','-of', 'COG', out_vrt, out_file,
    '-co', 'TILING_SCHEME=GoogleMapsCompatible','-co','COMPRESS=DEFLATE','-co',f'NUM_THREADS={cpu}','-a_nodata','-9999'])
    #gdal_translate -of COG merged.vrt merged_cog.tif ^
    #-co COMPRESS=DEFLATE -co PREDICTOR=2 -co NUM_THREADS=ALL_CPUS ^
    #-a_nodata -9999
    return out_file

def get_orgin_xy(rasterfile):
    ''' Returns [(ll_x,ll_y),(ur_x,ur_y)] Lower Left , Upper Right coordinate of rasterfile'''
    p = sp.run(['gdalinfo','-json',rasterfile],capture_output=True)
    result = json.loads(p.stdout)
    corner_coordinates = result.get("cornerCoordinates")
    return [corner_coordinates.get('lowerLeft'),corner_coordinates.get('upperRight')]

def get_las_bounds(lasfile):
    # get rounded to metre bounds of lidar file (las,laz)
    p = sp.run(['pdal','info','--metadata',lasfile],capture_output=True)
    info_json = json.loads(p.stdout.decode('UTF-8'))
    metadata = info_json.get("metadata")
    minx = ceil(metadata.get("minx"))
    miny = ceil(metadata.get("miny"))
    maxx = floor(metadata.get("maxx"))
    maxy = floor(metadata.get("maxy"))
    return ([minx,maxx],[miny,maxy])

def create_hillshade(dem,output_tif):
    '''Create hillshade (COG) from input dem
    '''
    logger.info(f"Start Hillshade calc and COG creation: --> {output_tif}")
    cpu = cpu_count() - 4
    p = sp.run(['gdaldem','hillshade',dem,output_tif,
    '-alg','ZevenbergenThorne',
    '-of','COG',
    '-co', 'TILING_SCHEME=GoogleMapsCompatible',
    '-co','BLOCKSIZE=1024',
    '-co','COMPRESS=DEFLATE',
    '-co',f'NUM_THREADS={cpu}'],capture_output=True,shell=True)

    if not p.returncode == 0:
        logger.error("Error running COG creation of Hillshade",p.stdout,p.stderr)
    logger.info("Done CHM calc and COG creation")


if __name__ == "__main__":
    ps = sys.argv[1]
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    result = create_chm_by_letterblock(letter_block=ps,index='file')
    


    



