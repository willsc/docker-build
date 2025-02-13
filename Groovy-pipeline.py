pipeline {
    agent any

    // Trigger on GitHub Pull Request events if applicable.
    triggers {
        githubPullRequest(
            triggerPhrase: 'retest this please',
            useGitHubHooks: true,
            permitAll: true
        )
    }

    environment {
        // Nexus Docker registry settings.
        NEXUS_REGISTRY      = "https://nexus.example.com:8082"   // Replace with your Nexus Docker registry URL.
        NEXUS_DOCKER_CREDS  = "nexus-docker-credentials"         // Jenkins credentials ID for Nexus Docker.

        // Nexus Helm repository settings.
        NEXUS_HELM_REPO     = "https://nexus.example.com/repository/helm-hosted/"
        NEXUS_HELM_CREDS    = "nexus-helm-credentials"           // Jenkins credentials ID (username/password) for Nexus Helm repo.

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
                    // The version must follow semantic versioning (e.g., 1.2.3) and should not be a SNAPSHOT.
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
                    // Build the Docker image using the semantic version as the tag.
                    dockerImage = docker.build("my-java-app:${APP_VERSION}")
                }
            }
        }

        stage('Security Scan') {
            steps {
                // Example using Trivy to scan the Docker image.
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
                // Add additional QA validations (smoke tests, etc.) as needed.
            }
        }

        stage('Push Docker Image to Nexus') {
            steps {
                script {
                    // Log in to Nexus and push the Docker image.
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
                    // Update the Helm chart's Chart.yaml with the semantic version.
                    sh """
                        sed -i 's/^version:.*/version: ${APP_VERSION}/' ${HELM_CHART_PATH}/Chart.yaml
                        sed -i 's/^appVersion:.*/appVersion: ${APP_VERSION}/' ${HELM_CHART_PATH}/Chart.yaml
                    """
                    // Package the Helm chart; the resulting .tgz file will include the version.
                    sh "helm package ${HELM_CHART_PATH} -d helm-packages"
                }
            }
        }

        stage('Push Helm Chart to Nexus') {
            steps {
                script {
                    // Locate the packaged Helm chart (.tgz file).
                    def helmPackage = sh(
                        script: "ls helm-packages/*.tgz",
                        returnStdout: true
                    ).trim()
                    echo "Found Helm package: ${helmPackage}"

                    // Push the Helm package to Nexus.
                    // This example uses curl; adjust if you use a helm push plugin.
                    withCredentials([usernamePassword(
                        credentialsId: "${NEXUS_HELM_CREDS}",
                        usernameVariable: 'HELM_USER',
                        passwordVariable: 'HELM_PASS'
                    )]) {
                        // Extract the filename from the path.
                        def packageName = helmPackage.tokenize('/').last()
                        sh """
                            curl -v -u ${HELM_USER}:${HELM_PASS} --upload-file ${helmPackage} ${NEXUS_HELM_REPO}${packageName}
                        """
                    }
                }
            }
        }

        stage('Deploy to Staging with Helm') {
            when {
                // Only deploy if this is not a pull request build.
                expression { return env.CHANGE_ID == null }
            }
            steps {
                script {
                    // Locate the packaged Helm chart.
                    def helmPackage = sh(
                        script: "ls helm-packages/*.tgz",
                        returnStdout: true
                    ).trim()
                    echo "Deploying ${helmPackage} to staging cluster..."
                    // Deploy to the staging Kubernetes cluster.
                    // Ensure your Jenkins agent has access to the staging cluster (e.g., via kubeconfig).
                    // For example, using a specific kube-context for staging:
                    sh "helm upgrade --install my-java-app ${helmPackage} --namespace staging --create-namespace --kube-context staging"
                }
            }
        }

        stage('Approval Gate for Production Deployment') {
            when {
                // Only request approval for non-PR builds.
                expression { return env.CHANGE_ID == null }
            }
            steps {
                input message: 'Approve deployment to production?', ok: 'Deploy Production'
            }
        }

        stage('Deploy to Production with Helm') {
            when {
                // Only deploy if this is not a pull request build.
                expression { return env.CHANGE_ID == null }
            }
            steps {
                script {
                    def helmPackage = sh(
                        script: "ls helm-packages/*.tgz",
                        returnStdout: true
                    ).trim()
                    echo "Deploying ${helmPackage} to production cluster..."
                    // Deploy to the production Kubernetes cluster.
                    // Ensure your Jenkins agent can access the production cluster (e.g., via kubeconfig).
                    // For example, using a specific kube-context for production:
                    sh "helm upgrade --install my-java-app ${helmPackage} --namespace production --create-namespace --kube-context production"
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
