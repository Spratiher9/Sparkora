from pyspark.shell import spark
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os
from copy import deepcopy
from pyspark.ml.feature import Imputer
from IPython import get_ipython


class Sparkora:
    def __init__(self, data=None, output=None):
        """
        Sparkora core class. Entry point to the Sparkora functionality.

        :param data: can be dataframe or path to csv.
        :param output: output data column(s).
        """
        self.snapshots = {}
        self.logs = []
        self.configure(data=data, output=output)
        self.data = data
        self.output = output
        self.training_data = None
        self.testing_data = None

    def configure(self, data=None, output=None):
        """
        Method for configuring the Sparkora object.

        :param data: data on which Sparkora object will be wrapped.
        :param output: output column name.
        :return: None
        """
        if (type(output) is str):
            self.output = output
        if (type(data) is str):
            self.initial_data = spark.read.csv(data)
            self.data = self.initial_data
            self.logs = []
        if (type(data) is DataFrame):
            self.initial_data = data
            self.data = data
            self.logs = []

    def remove_feature(self, feature_name):
        """
        Method for removing a feature.

        :param feature_name: name of the feature to be dropped.
        :return: None
        """
        self.data = self.data.drop(feature_name)
        self._log("self.remove_feature('{0}')".format(feature_name))

    def extract_feature(self, old_feat, new_feat, mapper):
        """
        Method for extracting new feature from existing feature(s).

        :param old_feat: feature(s) which will be used to build the new feature.
        :param new_feat: new feature's name.
        :param mapper: the logic to be applied for the creation of the new feature.
        :return: None
        """
        if type(old_feat) is str:
            self.data = self.data.withColumn(new_feat, mapper(self.data[old_feat]))
        else:
            self.data = self.data.withColumn(new_feat, mapper(*old_feat))
        self._log("self.extract_feature({0}, {1}, {2})".format(old_feat, new_feat, mapper))

    def impute_missing_values(self, strategy="mean"):
        """
        Method for imputing missing values with a chosen strategy of imputation.
        There are 3 strategies for imputation:

            1. mean
            2. median
            3. mode

        :param strategy: type of strategy to be used for imputation, by default mean is used.
        :return: None
        """
        column_names = self.input_columns()
        imp = Imputer(inputCols=column_names, outputCols=column_names).setStrategy(strategy)
        self.data = imp.fit(self.data).transform(self.data)
        self._log("self.impute_missing_values({0})".format(strategy))

    def scale_input_values(self):
        """
        Method for normalizing the data with Max-Min Scaler.

        :return: None
        """
        column_names = self.input_columns()
        for col in column_names:
            values = self.data.agg(F.max(col).alias("max_value"), F.min(col).alias("min_value")).collect()[0]
            max_value = values["max_value"]
            min_value = values["min_value"]
            denom = max_value - min_value
            scaler_udf = F.udf(lambda x: (x - min_value) / denom, T.DoubleType())
            self.data = self.data.withColumn(col, scaler_udf(self.data[col]))
        self._log("self.scale_input_values()")

    def extract_ordinal_feature(self, feature_name):
        """
        Method for one hot encoding and extracting the encoded features for a given feature.

        :param feature_name: feature to be parsed and extracted from.
        :return: None
        """
        categories = self.data.select(feature_name).distinct().rdd.flatMap(lambda x: x).collect()
        for category in categories:
            self.data = self.data.withColumn("{}={}".format(feature_name, category),
                                             F.when(F.col(feature_name) == category, F.lit(1)).otherwise(0))
        self.data = self.data.drop(feature_name)
        self._log("self.extract_ordinal_feature('{0}')".format(feature_name))

    def set_training_and_testing(self, train_size):
        """
        Method for splitting the data into training and testing sets.

        :param train_size: floating point value indicating the amount of data to be kept in training set (remaining
        in testing set).
        :return: None
        """
        test_size = 1 - train_size
        self.training_data, self.testing_data = self.data.randomSplit(weights=[train_size, test_size], seed=200)
        self._log("self.set_training_and_testing({})".format(train_size))

    def plot_feature(self, feature_name):
        """
        Method for plotting feature against the output column. (Available only for Databricks)

        :param feature_name: feature to be plotted against output.
        :return: None
        """
        if "DATABRICKS_RUNTIME_VERSION" in os.environ.keys():
            display = get_ipython().user_ns['display']
            display(self.data.select(feature_name,self.output))
            self._log("self.plot_feature('{}')".format(feature_name))
        else:
            raise Exception("plotting feature's only available in Databricks 💀")


    def input_columns(self):
        """
        Method for setting the input columns in the dataset.

        :return: list of input column names
        """
        column_names = self.data.columns
        column_names.remove(self.output)
        return column_names

    def explore(self):
        """
        Method for plotting the features of the dataset against the output(s) of the dataset.
        Best viewed in databricks runtime 10.0.0 and above.

        :return: None
        """
        if "DATABRICKS_RUNTIME_VERSION" in os.environ.keys():
            features = self.input_columns()
            self._log("self.explore() begins")
            for feature in features:
                self.plot_feature(feature)
            self._log("self.explore() ends")
        else:
            raise Exception("explore only available in Databricks 💀")

    def snapshot(self, name):
        """
        Method for taking a snapshot of the current state of the data in the Sparkora object.

        :param name: name of the snapshot by which we can refer later if required.
        :return: None
        """
        snapshot = {
            "data": self.data,
            "logs": deepcopy(self.logs)
        }
        self.snapshots[name] = snapshot


    def use_snapshot(self, name):
        """
        Method for restoring back to a particular state.

        :param name: name of the snapshot to which the state has to be restored to
        :return: None
        """
        self.data = self.snapshots[name]["data"]
        self.logs = self.snapshots[name]["logs"]

    def _log(self, string):
        """
        Method for storing operation statements as part of logging.

        :param string: the statement to be logged
        :return:
        """
        self.logs.append(string)
