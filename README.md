## Purpose

To estimate AWS Aurora Serverless v2 costs based on Provisioned usage. Aurora Capacity Units (ACUs) are inferred based on Aurora Provisioned instance parameters and CloudWatch usage metrics.

Multivariate Regression is used to examine variables such as CPU (provisioned CPU capacity), memory (provisioned memory capacity), provisioned_util (provisioned CPU utililization), and ServerlessDatabaseCapacity (serverless capacity units). Regression predictive modeling helps determine a numeric value. In this usecase, the numeric value is the Serverless Capcity Unit count (ACU). 


**Training Steps**
- simulate identical workloads to a provisioned and serverless environment (e.g. via pgbench or hammerdb)
- execute training python script, script steps
  - read in input list (via CSV) of Aurora Provisioned instances
  - download pricing list with instance details from the AWS Price List Bulk API 
  - determine provisioned instance memory and CPU details
  - get provisioned and serverless usage data (via CloudWatch) for the specified time period
  - split and train data via the Extreme Gradient Boosting (XGBoost) library
  - save model to file


**Inference Steps - Customer account**
- execute inference python script, script steps
  - read in input list (via CSV) of Aurora Provisioned instances
  - download pricing list with instance and Reserved Instance (RI) details from the AWS Price List Bulk API 
  - determine provisioned instance memory and CPU details
  - get provisioned usage data (via CloudWatch) for the specified time period
  - read in trained model and ifer ACU values 
  - monthly cost values are calculated and savings compared to the specified RI term cost
  - CSV output to file: provisioned_instance,provisioned_util,provisioned_vcpu,provisioned_mem,serverless_acu,provisioned_monthly_cost,serverless_monthly_cost,serverless_savings

## Setup and Usage

**Training**
- install libraries 
  ```py
  pip install -r requirements-train.txt
  ```
- specify days back from current
  ```py
  python train-get-metrics.py -d 4
  ```
- specify time range of data 
  ```py
  python train-get-metrics.py -s '2022-07-07 02:00:00' -e '2022-07-10 02:00:00'
  ``` 

**Inference**
- install libraries 
  ```py
  pip install -r requirements-inference.txt
  ```
- specify days back from current
  ```py
  python inference-get-metrics.py -d 4
  ```
- specify time range of data 
  ```py
  python inference-get-metrics.py -s '2022-06-25 02:00:00' -e '2022-07-12 02:00:00'
  ``` 

_For more examples, please refer to the [Documentation](https://somerepo.com)_

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Roadmap

- [ ] Add ability to include costs estimates for Aurora IO usage charges 
- [ ] Add ability to include PPA pricing discount

See the [open issues](https://somerepo.com) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement". Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/UsefulFeature`)
3. Commit your Changes (`git commit -m 'Add some UsefulFeature'`)
4. Push to the Branch (`git push origin feature/UsefulFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Contact

Eric Garcia - grmeri@amazon.com

Project Link: [https://somerepo.com](https://somerepo.com)



