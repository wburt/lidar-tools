# lidar-tools
some experiments for processing lots of LiDAR into derivatives

## CHM
tool for producing CHM by mapsheet or letterblock using availaible lidar by index. 

Example usage to calculate chm for calculating all of 082K mapsheets:
```
python -m process_letterblcok 082k
```

Example usage to calculate all mapsheets like 082k02%
```
python -m process_letterblock 082k02
```

Example usage to calculate only mapsheet 082k023
```
python -m process_letterblock 082k02
```