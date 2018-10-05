package amqp

import (
	"os/exec"
	"testing"
)

// relativePath takes a relative path inside this repository and returns
// the full path to the given destination
func relativePath(dst string) string {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		var err error
		gopath, err = homedir.Expand("~/go")
		if err != nil {
			panic(err)
		}
	}
	firstPath := strings.Split(gopath, ":")[0]
	return p.Join(firstPath, "src/github.com/graphite-ng/carbon-relay-ng", dst)
}

func TestMain(m *testing.M) {
	log.Println("launching docker-dev stack...")
	version := exec.Command("docker-compose", "version")
	output, err := version.CombinedOutput()
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(string(output))

	cmd := exec.Command("docker-compose", "up", "--force-recreate")
	cmd.Dir = relativePath("stacktest/amqp")

	readPipe := func(in io.ReadCloser) {
		scanner := bufio.NewScanner(in)
		for scanner.Scan() {
			// consume pipes, even if we don't look at the output
		}
	}

	go readPipe(cmd.StdoutPipe())
	go readPipe(cmd.StderrPipe())

	err = cmd.Start()
	if err != nil {
		log.Fatal(err.Error())
	}

	retcode := m.Run()

	log.Println("stopping docker-compose stack...")
	cmd.Process.Signal(syscall.SIGINT)
	err = cmd.Wait()

	// 130 means ctrl-C (interrupt) which is what we want
	if err != nil && err.Error() != "exit status 130" {
		log.Printf("ERROR: could not cleanly shutdown running docker-compose command: %s", err)
		retcode = 1
	} else {
		log.Println("docker-compose stack is shut down")
	}

	os.Exit(retcode)
}
