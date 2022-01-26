pipeline {
  triggers { pollSCM('*/5 * * * *') }
  agent { label "data_super_store" }
  stages {
    stage("Register to Prefect"){
      steps{ sh '''#!/bin/bash
        export PATH="/home/ubuntu/miniconda3/bin:/home/ubuntu/bin:/home/ubuntu/.local/bin:$PATH"

        # Please add Prefect Labels for Project Here
        export PGR_PREFECT_LABEL="enterlabelhere"

        pgr_prefect shell register
      '''} // end steps
     } // end stage: Register to Prefect
  } // end stages
} // end pipeline
