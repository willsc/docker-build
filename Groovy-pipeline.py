pipeline {
    agent any

    // Trigger on pull request events (if using GitHub/Bitbucket plugins).
    triggers {
        githubPullRequest(
            triggerPhrase: 'retest this please',
            useGitHubHooks: true,
            permitAll: true
        )
    }

    environment {
        // Nexus Docker registry settings.
        NEXUS_REGISTRY      = "https://nexus.example.com:8082" // Replace with your Nexus Docker registry URL.
        NEXUS_DOCKER_CREDS  = "nexus-docker-credentials"       // Jenkins credentials ID for Nexus Docker registry.

        // Nexus Helm repository settings.
        NEXUS_HELM_REPO     = "https://nexus.example.com/repository/helm-hosted/"
        NEXUS_HELM_CREDS    = "nexus-helm-credentials"         // Jenkins credentials ID (username/password) for Nexus Helm repo.

        // Path to your Helm chart in the repository.
        HELM_CHART_PATH     = "helm/my-java-app"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Determine Semantic Version') {
            steps {
                script {
                    // Extract the semantic version from the Maven pom.xml.
                    // This assumes that the pom.xml contains a version like "1.2.3" (non-SNAPSHOT for production).
                    env.APP_VERSION = sh(
                        script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout",
                        returnStdout: true
                    ).trim()
                    echo "Application Semantic Version: ${env.APP_VERSION}"
                }
            }
        }

        stage('Build Java Application') {
            steps {
                sh 'mvn clean package'
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    // Build the Docker image tagged with the semantic version.
                    dockerImage = docker.build("my-java-app:${APP_VERSION}")
                }
            }
        }

        stage('Security Scan') {
            steps {
                // Run a security scan (example uses Trivy).
                sh "trivy image my-java-app:${APP_VERSION}"
            }
        }

        stage('Integration Tests') {
            steps {
                sh 'mvn verify -Pintegration-tests'
            }
        }

        stage('QA Release') {
            steps {
                echo "Performing QA validations..."
                // Add any QA validation steps, e.g., smoke tests or deployment to a QA environment.
            }
        }

        stage('Push Docker Image to Nexus') {
            steps {
                script {
                    // Push the Docker image to the Nexus Docker registry.
                    docker.withRegistry("${NEXUS_REGISTRY}", "${NEXUS_DOCKER_CREDS}") {
                        dockerImage.push("${APP_VERSION}")
                        // Optionally, push the "latest" tag.
                        dockerImage.push("latest")
                    }
                }
            }
        }

        stage('Package Helm Chart') {
            steps {
                script {
                    // Update the Helm chart's Chart.yaml to use the semantic version.
                    sh """
                        sed -i 's/^version:.*/version: ${APP_VERSION}/' ${HELM_CHART_PATH}/Chart.yaml
                        sed -i 's/^appVersion:.*/appVersion: ${APP_VERSION}/' ${HELM_CHART_PATH}/Chart.yaml
                    """
                    // Package the Helm chart; the resulting .tgz file will contain the version.
                    sh "helm package ${HELM_CHART_PATH} -d helm-packages"
                }
            }
        }

        stage('Push Helm Chart to Nexus') {
            steps {
                script {
                    // Locate the packaged Helm chart (.tgz file) from the helm-packages directory.
                    def helmPackage = sh(
                        script: "ls helm-packages/*.tgz",
                        returnStdout: true
                    ).trim()
                    echo "Found Helm package: ${helmPackage}"

                    // Push the Helm package to Nexus.
                    // This example uses a curl command. Adjust if you have a helm-push plugin available.
                    withCredentials([usernamePassword(
                        credentialsId: "${NEXUS_HELM_CREDS}",
                        usernameVariable: 'HELM_USER',
                        passwordVariable: 'HELM_PASS'
                    )]) {
                        // Extract the file name from the package path.
                        def packageName = helmPackage.tokenize('/').last()
                        sh """
                            curl -v -u ${HELM_USER}:${HELM_PASS} --upload-file ${helmPackage} ${NEXUS_HELM_REPO}${packageName}
                        """
                    }
                }
            }
        }

        stage('Approval Gate') {
            steps {
                input message: 'Approve deployment to production?', ok: 'Deploy'
            }
        }

        stage('Deploy to Kubernetes with Helm') {
            when {
                // Only deploy if this is not a pull request build.
                expression { return env.CHANGE_ID == null }
            }
            steps {
                script {
                    // Find the packaged Helm chart and deploy (or upgrade) using Helm.
                    def helmPackage = sh(
                        script: "ls helm-packages/*.tgz",
                        returnStdout: true
                    ).trim()
                    sh "helm upgrade --install my-java-app ${helmPackage} --namespace production --create-namespace"
                }
            }
        }
    }

    post {
        always {
            echo "Pipeline completed."
        }
        failure {
            echo "Pipeline failed."
        }
    }
}
