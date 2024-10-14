# Utils notebook for generate files from querys on AWS Redshift

## Ensure you install the requirements

## If you are using docker run this code below
```
docker run -it --rm --network my-net -p 8888:8888 -v /home/hcunha/dev/py/jupyter/notebook-redshift-file-generator-util:/home/jovyan/work jupyter/pyspark-notebook
```

## If you'll run by bash pass the arguments 
```
python redshift_file_generator_by_chunks.py --username "herculano_cunha" --password "your_pass" --query "select * from credit_portfolio.metrica limit 100" --filename "test.csv" --filepath "/home/jovyan/work/files"
```

## The .bat file it's for windows OS users only