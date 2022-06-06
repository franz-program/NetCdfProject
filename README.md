# NetCdfProject

How to test:

0) download code
1) add libraries not in maven: https://drive.google.com/drive/folders/1fjBNADbJUH79MOuBuDIXxXniDmLsQLTK?usp=sharing
2) download test files: https://drive.google.com/drive/folders/1OYtGfh3E7BvFvUpe--VYHdONfAzw5Rjp?usp=sharing
3) set the max heap size of the JVM
4) do some fast pretests: call WritingOnOutputFileTester.main with just one argument: any directory path where all the test files will be put
5) if the test succeeds then start testing the time: call TimeTester.main with 5 arguments:
    -1 full path of txt config file 1 with one row: the path of folder downloaded at 2)
    -2 full path of txt config file 2 contained in folder downloaded at 2)
    -3 any full path for the output file (csv)
    -4 any full path for header file (csv)
    -5 any full path for log file (txt)
6) wait for results (estimate = 10h)
