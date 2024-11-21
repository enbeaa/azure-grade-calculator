# Prerequisites
- Azure Storage Account with
  - Container named 'students'
  - Container named 'results'
  
- Development Environment: Visual Studio Code
- Azure Functions Core Tools

# Setup
1. Clone repository
2. Install dependencies
```
pip install -r requirements.txt
```
4. Change local.settings.json "AzureWebJobsStorage" and "myfreedbresourcegroaebb_STORAGE" to have your storage account key as the value
5. In 'Run and Debug', click 'Attach to Python Functions' to start running the function app

# Testing
- The **test_inputs** folder contains multiple csv files that can be used to test the code.
- Upload one or more of these to your 'student' container
- The output file should be uploaded to your ''results' container
