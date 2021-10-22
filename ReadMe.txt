Last update: Aug 20 2021

This folder contains the dockerfile and the python program to update 
dynamo on the neustar leads. This ReadMe will go over everything you 
need to know to be able to update the main.py file and upload it 
successfully to aws.

The whole process of how the program works is as follows: 
- First the csv file that wants to be analyzed has to be placed on 
the s3 bucket named "incoming-external-emails-prod" (there is a incoming-external-emails-dev 
for testing purposes). 

- Once a new csv file is uploaded to the s3 bucket, it triggers a lambda function 
called “incoming_email_csv_processing-prod” (there is a incoming_email_csv_processing-dev 
for testing as well). This lambda function will get the key for the s3 file that 
was uploaded and sends that key to a new batch job it creates.

The code of the lambda function is on github: 
https://github.com/Sonic-Web-Dev/monty-python/tree/master/lambdas/incoming_email_CSV_processing
Any changes updated on github will automatically update it on the lambda function.

- The newly created batch job (which is always called analyze-csv) uses 
the “professor-job-definition-prod:2” which contains the docker image that will 
run the csv analysis and update dynamo. 

The average time between file upload to the docker image running on batch is usually of 3 to 5 min.
Just keep watching the dashboard on the batch page for updates. Also, each job was a cloudwatch log which 
can be accessed to see errors or progress of the job.



If changes want to be made to the python file, the way to update it is as follows: 
- Make the wanted changes to the main.py file (if one wants to run the file locally, 
change the code in line 161, just change the value of “key” to the key value that 
is shown on the file in s3).

- If a new library is being used, the dockerfile needs to be updated. To do so, 
open the Dockerfile, and add the library name to line 3.

- To upload the file correctly to aws, go into the Elastic Container Registry page in aws. 
Go into the “professor-aws-batch-prod” repository (once again, there is a professor-aws-batch-dev as well).

- Look at the “View push commands”. Follow the instructions there and follow the 
correct operating system commands. Once done, you should refresh the page and find 
the new latest image tag. The following link is for a tutorial of this same process: 
https://www.youtube.com/watch?v=EoC46x0jAas 

(Note: this tutorial creates the entire batch environment from scratch, which is not 
needed since the environment is fully set up).


- This will automatically update all batch queues and definitions and you can now upload 
a file to the s3 bucket and check if everything works as intended.

