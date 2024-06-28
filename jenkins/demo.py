from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Initialize Spark session
spark = SparkSession.builder.appName("StarSchema").config("spark.jars", "postgresql-42.7.3.jar").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("./healthcare_dataset.csv", header=True)
# df.show()

# Patients table
patient_df = df.select("Name","Age","Gender","Blood Type","Medical Condition").distinct()
window_spec = Window.orderBy("Name")  # or order by any column that makes sense for your data
patient_df = patient_df.withColumn("PatientID", row_number().over(window_spec))
patient_df = patient_df.select("PatientID","Name","Age","Gender","Blood Type","Medical Condition")
patient_df.show()
patient_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.patient") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .save()

# Doctors table
doctor_df = df.select("Doctor").distinct().withColumn("DoctorID", monotonically_increasing_id()+ 1)
doctor_df.show()
doctor_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.doctor") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .save()


# Hospital table
hospital_df = df.select("Hospital").distinct().withColumn("HospitalID", monotonically_increasing_id()+ 1)
hospital_df.show()
hospital_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.hospital") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .save()

# InsuaranceProvider table
insurance_df = df.select("Insurance Provider").distinct().withColumn("InsuranceProviderID", monotonically_increasing_id()+ 1)
insurance_df.show()
insurance_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.insurance") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .save()

# Room table
room_df = df.select("Room Number").distinct().withColumn("RoomNumberID", monotonically_increasing_id()+ 1)
room_df.show()
room_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.room") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .save()

# billing table
billing_df = df.select("Billing Amount").distinct().withColumn("BillingID", monotonically_increasing_id()+ 1)
billing_df.show()
billing_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.billing") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .save()

# admission table
admission_df = df.join(patient_df, ["Name", "Age", "Gender", "Blood Type", "Medical Condition"]) \
                   .join(doctor_df, df.Doctor == doctor_df.Doctor) \
                   .join(hospital_df, df.Hospital == hospital_df.Hospital) \
                   .join(insurance_df, df["Insurance Provider"] == insurance_df["Insurance Provider"]) \
                   .join(room_df, df["Room Number"] == room_df["Room Number"]) \
                   .join(billing_df, df["Billing Amount"] == billing_df["Billing Amount"]) \
                   .select(
                    #    "RowID",
                       "PatientID",
                       "Date of Admission",
                       doctor_df["DoctorID"],
                       hospital_df["HospitalID"],
                       insurance_df["InsuranceProviderID"],
                       billing_df["BillingID"],
                       room_df["RoomNumberID"].alias("RoomNumber"),
                       "Admission Type",
                       "Discharge Date",
                       "Medication",
                       "Test Results"
                   )
admission_df.show()
admission_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.admission") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .save()