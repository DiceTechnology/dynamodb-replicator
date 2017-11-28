node {
  	checkout scm
  	def maven = docker.image("maven:3-alpine")

	withCredentials([usernamePassword(credentialsId: 'aws_key', usernameVariable: 'AWS_ACCESS_KEY_ID', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
		stage('Build && run unit tests') {
			maven.inside() {
				sh "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY mvn -B clean install"
			}
		}
	}
}

