from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

print("Démarrage de Spark...")
spark = SparkSession.builder.appName("TrainBikeModel").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Chargement des données depuis HDFS...")
df = spark.read.parquet("hdfs://namenode:9000/data/bike/raw")

# --- CORRECTION ICI : On supprime TOUTES les lignes qui contiennent au moins un NULL ---
df = df.na.drop() 

# Préparation des variables explicatives
feature_cols = ["season", "yr", "mnth", "hr", "holiday", "weekday", "workingday", "weathersit", "temp", "atemp", "hum", "windspeed"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# On assemble les features et on garde 'cnt' (la cible) et 'yr' (l'année pour le découpage)
df_ml = assembler.transform(df).select("features", "cnt", "yr")

# DÉCOUPAGE TEMPOREL : Train = 2011 (yr=0), Test = 2012 (yr=1)
train_data = df_ml.filter(df_ml.yr == 0).drop("yr")
test_data = df_ml.filter(df_ml.yr == 1).drop("yr")

print(f"Données d'entraînement (2011) : {train_data.count()} lignes")
print(f"Données de test (2012) : {test_data.count()} lignes")

# Outils d'évaluation
evaluator_rmse = RegressionEvaluator(labelCol="cnt", predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol="cnt", predictionCol="prediction", metricName="mae")
evaluator_r2 = RegressionEvaluator(labelCol="cnt", predictionCol="prediction", metricName="r2")

print("\nEntraînement des 3 modèles (Patientez quelques secondes)...")

# --- Modèle 1 : Régression Linéaire ---
lr = LinearRegression(featuresCol="features", labelCol="cnt")
lr_model = lr.fit(train_data)
lr_preds = lr_model.transform(test_data)
print("\nRégression Linéaire :")
print(f"   RMSE: {evaluator_rmse.evaluate(lr_preds):.2f} | MAE: {evaluator_mae.evaluate(lr_preds):.2f} | R2: {evaluator_r2.evaluate(lr_preds):.2f}")

# --- Modèle 2 : Random Forest ---
rf = RandomForestRegressor(featuresCol="features", labelCol="cnt", numTrees=30)
rf_model = rf.fit(train_data)
rf_preds = rf_model.transform(test_data)
print("\nRandom Forest :")
print(f"   RMSE: {evaluator_rmse.evaluate(rf_preds):.2f} | MAE: {evaluator_mae.evaluate(rf_preds):.2f} | R2: {evaluator_r2.evaluate(rf_preds):.2f}")

# --- Modèle 3 : GBT (Gradient-Boosted Trees) ---
gbt = GBTRegressor(featuresCol="features", labelCol="cnt", maxIter=30)
gbt_model = gbt.fit(train_data)
gbt_preds = gbt_model.transform(test_data)
print("\nGBT (Gradient-Boosted Trees) :")
print(f"   RMSE: {evaluator_rmse.evaluate(gbt_preds):.2f} | MAE: {evaluator_mae.evaluate(gbt_preds):.2f} | R2: {evaluator_r2.evaluate(gbt_preds):.2f}")

# On sauvegarde les 3 modèles dans HDFS (on choisira le meilleur après)
print("\n💾 Sauvegarde des modèles dans HDFS...")
lr_model.write().overwrite().save("hdfs://namenode:9000/models/lr_model")
rf_model.write().overwrite().save("hdfs://namenode:9000/models/rf_model")
gbt_model.write().overwrite().save("hdfs://namenode:9000/models/gbt_model")
print("Terminé ! Modèles sauvegardés.")