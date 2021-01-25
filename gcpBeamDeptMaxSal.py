import csv
import apache_beam as beam
from apache_beam.io import ReadFromText,WriteToText


def print_row(element):
    print(element)

def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    return line

class CollectData(beam.DoFn):
    def process(self, element):
        """
        Returns a list of tuples containing DeptNo, DepartmentName, Salary
        """
        result = [
            "{},{},{}".format(
                element[0],element[1][1][0],element[1][0][0])
        ]
        return result

class TupToDict(beam.DoFn):
    def process(self, element):
            di = {'firstname': element[0], 'lastname': element[1], 'salary': element[2], 'deptno': element[3]}
            return [di]

class TupToDictDept(beam.DoFn):
    def process(self, element):
            di = {'deptno': element[0], 'deptname': element[1][1][0], 'salary': element[1][0][0]}
            return [di]


with beam.Pipeline() as p:
    emp = (p
           | "label1" >> beam.io.ReadFromText("gs://gcpbucket_2021/spark_emp_rdd_df_ds_data.csv",skip_header_lines=1)
           | "parse1" >> beam.Map(parse_file)
           | "JSON" >>  beam.ParDo(TupToDict())
           #| beam.Map(lambda emp: (emp[3] , int(emp[2])))
           #| beam.CombinePerKey(lambda  x : sum(x))
           #| "print1" >> beam.Map(print_row)
           )
    project_id = "psychic-medley-299402"  # replace with your project ID
    dataset_id = 'apachebeam_demo'  # replace with your dataset ID
    table_id = 'employee1'  # replace with your table ID
    table_schema2 = 'firstname:STRING, lastname:STRING, salary:INTEGER, deptno:INTEGER'

    # Persist to BigQuery
    # WriteToBigQuery accepts the data as list of JSON objects

with beam.Pipeline() as p:
    emp1 = (p
           | "label1" >> beam.io.ReadFromText("spark_emp_rdd_df_ds_data.csv", skip_header_lines=1)
           #| 'ReadRequestFile' >> ReadFromText("spark_emp_rdd_df_ds_data.csv", skip_header_lines=1)
           | "parse1" >> beam.Map(parse_file)
           | beam.Map(lambda emp: (emp[3] , int(emp[2])))
           | beam.CombinePerKey(lambda  x : sum(x))
           #| "print1" >> beam.Map(print_row)
           )

    dept1 = (p
            | "label2" >> beam.io.ReadFromText("spark_dept_rdd_df_ds_data.csv",skip_header_lines=1)
            | "parse2" >> beam.Map(parse_file)
            | beam.Map(lambda dept: (dept[0], dept[1]))
            #| "print2" >> beam.Map(print_row)
            )

    merged = ( emp1 , dept1) | beam.CoGroupByKey() | beam.ParDo(TupToDictDept())

    project_id = "psychic-medley-299402"  # replace with your project ID
    dataset_id = 'apachebeam_demo'  # replace with your dataset ID
    table_id = 'deptsal'  # replace with your table ID
    table_schema = 'deptno:STRING, deptname:STRING,salary:INTEGER'

    # Persist to BigQuery
    # WriteToBigQuery accepts the data as list of JSON objects
    merged | 'Write' >> beam.io.WriteToBigQuery(
                    table=table_id,
                    dataset=dataset_id,
                    project=project_id,
                    schema=table_schema,
                    custom_gcs_temp_location='gs://gcpbucket_2021/temp',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=int(100)
                    )