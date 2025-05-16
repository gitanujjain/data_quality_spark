from abc import ABC, abstractmethod
from pydeequ.checks import *
from pydeequ.verification import VerificationSuite, VerificationResult 


class Validation(ABC):
    def __init__(self,spark, df, column,add_info = {}):
        self. spark= spark
        self.df =df 
        self.column = column
        self.add_info = add_info
        self.value = add_info.get('value')
        
    @abstractmethod
    def test(self):
        pass


class UniquenessValidation(Validation):
    def __init__(self, spark, df,column,  add_info = {}):
        super().__init__(spark, df, column,  add_info)

    def test(self):
        try:
            check = Check(self.spark, CheckLevel.Warning, f"DQ for uniqueness for column {self.column}")
            checkResult = VerificationSuite(self.spark).onData(self.df).addCheck(check.isUnique(column=self.column)).run()
            checkResult_df = VerificationResult.successMetricsAsJson(self.spark, checkResult)
            print("--------------------UniquenessValidation", self.column, checkResult_df)
            return checkResult_df        
        except Exception as errr:
            return []

class PrimaryKeyValidation(Validation):
    def __init__(self, spark, df,column,  add_info = {}):
        super().__init__(spark, df, column,  add_info)

    def test(self):
        check = Check(self.spark, CheckLevel.Warning, f"DQ for primarykey for column {self.column}")
        checkResult = VerificationSuite(self.spark).onData(self.df).addCheck(check.isUnique(column=self.column).isComplete(column=self.column)).run()
        checkResult_df = VerificationResult.successMetricsAsJson(self.spark, checkResult)
        print("--------------------PrimaryKeyValidation", self.column, checkResult_df)
        return checkResult_df       


class NotNullValidation(Validation):
    def __init__(self, spark, df,column,  add_info = {}):
        super().__init__(spark, df, column,  add_info)

    def test(self):
        check = Check(self.spark, CheckLevel.Warning, f"DQ for not null for column {self.column}")
        checkResult = VerificationSuite(self.spark).onData(self.df).addCheck(check.isComplete(column=self.column)).run()
        checkResult_df = VerificationResult.successMetricsAsJson(self.spark, checkResult)
        print("--------------------NotNullValidation", self.column, checkResult_df)
        return checkResult_df 


class ContainEmailValidation(Validation):
    def __init__(self, spark, df,column,  add_info = {}):
        super().__init__(spark, df, column,  add_info)
    def test(self):
        check = Check(self.spark, CheckLevel.Warning, f"DQ for email constrain for column {self.column}")
        checkResult = VerificationSuite(self.spark).onData(self.df).addCheck(check.containsEmail(self.column,lambda x: x==1)).run()
        checkResult_df = VerificationResult.successMetricsAsJson(self.spark, checkResult)
        print("--------------------ContainEmailValidation", self.column, checkResult_df)
        return checkResult_df


class ContainValuesValidation(Validation):
    def __init__(self, spark, df,column,  add_info = {}):
        super().__init__(spark, df, column,  add_info)
    def test(self):
        check = Check(self.spark, CheckLevel.Warning, f"DQ for check constraint  column {self.column}")
        checkResult = VerificationSuite(self.spark).onData(self.df).addCheck(check.isContainedIn(self.column,self.value)).run()
        checkResult_df = VerificationResult.successMetricsAsJson(self.spark, checkResult)
        print("--------------------ContainValuesValidation", self.column, checkResult_df)
        return checkResult_df 
    

class MaxValueValidation(Validation):
    def __init__(self, spark, df,column,  add_info = {}):
        super().__init__(spark, df, column,  add_info)
        

    def test(self):
        checkResult = VerificationSuite(self.spark).onData(self.df).addCheck(self.check.hasMax(self.column,lambda x: x==self.value)).run()
        checkResult_df = VerificationResult.successMetricsAsJson(self.spark, checkResult)
        print("--------------------MaxValueValidation", self.column, checkResult_df)
        return checkResult_df 


class DataTypeValidation(Validation):
    def __init__(self, spark, df,column,  add_info = {}):
        super().__init__(spark, df, column,  add_info)
        
    
    def test(self):
        check = Check(self.spark, CheckLevel.Warning, f"DQ for uniqueness for column {self.column}")
        data_type=self.value
        if self.value.lower() == 'string':
            self.value = ConstrainableDataTypes.String
        elif self.value.lower() == 'numeric':
            self.value = ConstrainableDataTypes.Numeric
        elif self.value.lower() == 'boolean':
            self.value = ConstrainableDataTypes.Boolean
        elif self.value.lower() == 'fractional':
            self.value = ConstrainableDataTypes.Fractional
        elif self.value.lower() == 'null':
            self.value = ConstrainableDataTypes.Null
        elif self.value.lower() == 'integral':
            self.value = ConstrainableDataTypes.Integral
        print(self.spark)
        print(self.df.printSchema(), self.df.show())
        checkResult = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasDataType(self.column,self.value)).run()
        checkResult_df = VerificationResult.successMetricsAsJson(self.spark, checkResult)
        print("--------------------DataTypeValidation", self.column, checkResult_df)
        if len(checkResult_df) == 0:
            print("Dat validation is not performed")
        else:
            if  data_type == 'boolean' : 
                for each  in checkResult_df:
                    if each['name'] == 'Histogram.ratio.Boolean': 
                        if each['value'] ==1.0:
                            each['name'] == data_type
                            print("1",[each])
                            return [each]
                        else:
                            each['name'] = data_type
                            each['value'] = 0.0
                            print("2",[each])
                            return [each]
            elif data_type == 'fractional':
                for each  in checkResult_df:
                    if each['name'] == 'Histogram.ratio.Fractional' :
                        if each['value'] ==1.0:
                            each['name'] == data_type
                            print("3",[each])
                            return [each]
                        else:
                            each['name'] = data_type
                            each['value'] = 0.0
                            print("4",[each])
                            return [each]
            elif data_type == 'integral': 
                for each  in checkResult_df:
                    if each['name'] == 'Histogram.ratio.Integral' :
                        if each['value'] ==1.0:
                            each['name'] = data_type
                            print("5",[each])
                            return [each]
                        else:
                            each['name'] = data_type
                            each['value'] = 0.0
                            print("6",[each])
                            return [each]
            elif data_type == 'string': 
                for each  in checkResult_df:
                    if each['name'] == 'Histogram.ratio.String': 
                        if each['value'] ==1.0:
                            each['name'] = data_type
                            print("7",[each])
                            return [each]
                        else:
                            each['name'] = data_type
                            each['value'] = 0.0
                            print("8",[each])
                            return [each]
            elif data_type == 'numeric' :
                value=0
                for each  in checkResult_df:      
                    if each['name'] == 'Histogram.ratio.Numeric' or each['name'] == 'Histogram.ratio.Fractional' or each['name'] == 'Histogram.ratio.Integral': 
                        value+=each['value']
                else:
                    if value ==1.0:
                        print(each)
                        each['name'] = data_type
                        each['value'] = 1.0
                        print("9",[each])
                        return [each]
                    else:
                        each['name'] = data_type
                        each['value'] = 0.0
                        print("10",[each])
                        return [each]
            else:
                return checkResult_df

class DataQuality:
    def __init__(self, spark, df, config_json):
        self.spark = spark
        self.df=df
        self.config_json = config_json

    def rule_mapping(self, dq_rule):
        return {"check_if_not_null" : "NotNullValidation", 
                "check_if_unique" : "UniquenessValidation", 
                "check_if_primary" : "PrimaryKeyValidation",
                "check_if_emils": "ContainEmailValidation",
                "check_email" : 'ContainEmailValidation',
                "check_max_value":"MaxValueValidation",
                "check_for_list_value" : "ContainValuesValidation",
                "check_data_type":"DataTypeValidation"}[dq_rule]

    def _get_expectation(self):
        class_obj = globals()[self.rule_mapping()]
        return class_obj(self.extractor_args)

      
    def run_test(self):
        result=[]
        for column in self.config_json:
            if column["dq_rule"] is None:
                continue
            for dq_rule in column["dq_rule"]:
                expectation_obj = globals()[self.rule_mapping(dq_rule["rule_name"])]
                expectation_instance = expectation_obj(self.spark, self.df, column["column_name"], dq_rule["add_info"])
                for each in expectation_instance.test():
                   result.append(each)
        print("--------after run test------", result)
        return result