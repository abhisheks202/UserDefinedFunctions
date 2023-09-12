from pyspark.sql.types import *
from pyspark.sql.functions import *

# Flattern the JSON files
def flatten(df):
    complex_fields = dict([(field.name, field.dataType)
                            for field in df.schema.complex_fields
                            if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields)!=0:
        col_name=list(complex_fields.keys())[0]
        print("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns to flatten structs
        if(type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add array elements as rows
        elif(type(complex_fields[col_name]) == StructType):
            df = df.withColumn(col_name,explode_outer(col_name))

        #recompute remaining complex fields
        complex_fields = dict([(field.name, field.dataType)
                          for field in df.schema.fields
                          if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

        return df