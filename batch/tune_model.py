from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

print("Démarrage du Tuning (Recherche des meilleurs paramètres)...")
spark = SparkSession.builder.appName("TuneBikeModel").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Préparation des données
df = spark.read.parquet("hdfs://namenode:9000/data/bike/raw").na.drop()
feature_cols = ["season", "yr", "mnth", "hr", "holiday", "weekday", "workingday", "weathersit", "temp", "atemp", "hum", "windspeed"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_ml = assembler.transform(df).select("features", "cnt", "yr")

train_data = df_ml.filter(df_ml.yr == 0).drop("yr")
test_data = df_ml.filter(df_ml.yr == 1).drop("yr")

# 2. Modèle à optimiser (GBT) et évaluateur
gbt = GBTRegressor(featuresCol="features", labelCol="cnt")
evaluator = RegressionEvaluator(labelCol="cnt", predictionCol="prediction", metricName="rmse")

# 3. Grille de paramètres à tester (3 profondeurs x 2 itérations = 6 combinaisons)
paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [5, 7, 9]) \
    .addGrid(gbt.maxIter, [20, 50]) \
    .build()

# 4. Validation croisée
tvs = TrainValidationSplit(estimator=gbt,
                           estimatorParamMaps=paramGrid,
                           evaluator=evaluator,
                           trainRatio=0.8)

print("Entraînement des 6 combinaisons de paramètres (Patientez ~1 minute)...")
tvs_model = tvs.fit(train_data)

# 5. Récupération du meilleur modèle
best_model = tvs_model.bestModel
print(f"\nMeilleurs paramètres trouvés :")
print(f"   - maxDepth : {best_model.getMaxDepth()}")
print(f"   - maxIter : {best_model.getMaxIter()}")

# 6. Évaluation finale sur les données de test
predictions = best_model.transform(test_data)
print(f"\nPerformance du modèle optimisé sur les données de test :")
print(f"   RMSE: {evaluator.evaluate(predictions):.2f}")

# 7. Sauvegarde du modèle final (qui sera utilisé pour le Streaming)
print("\nSauvegarde du modèle optimisé dans HDFS...")
best_model.write().overwrite().save("hdfs://namenode:9000/models/bike_gbt_best")
print("Modèle final prêt pour la Semaine 4 !")