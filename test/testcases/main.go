package testcases

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/test/server"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

var (
	account *types.IRODSAccount
)

func setup() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setup",
	})

	err := server.StartServer()
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account, err = server.GetLocalAccount()
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	rand.Seed(time.Now().UnixNano())
}

func setup_existing() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setup_existing",
	})

	var err error
	account, err = server.GetLocalAccount()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}

func shutdown() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "shutdown",
	})

	// empty global variables
	account = nil

	err := server.StopServer()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}

func GetTestAccount() *types.IRODSAccount {
	accountCpy := *account
	return &accountCpy
}

func makeFixedContentTestDataBuf(size int64) []byte {
	testval := "abcdefghijklmnopqrstuvwxyz"

	// fill
	dataBuf := make([]byte, size)
	writeLen := 0
	for writeLen < len(dataBuf) {
		copy(dataBuf[writeLen:], testval)
		writeLen += len(testval)
	}
	return dataBuf
}

func makeRandomContentTestDataBuf(size int64) []byte {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	// fill
	dataBuf := make([]byte, size)
	for i := range dataBuf {
		dataBuf[i] = letters[rand.Intn(len(letters))]
	}
	return dataBuf
}

func createLocalTestFile(name string, size int64) (string, error) {
	// fill
	dataBuf := makeFixedContentTestDataBuf(1024)

	f, err := ioutil.TempFile("", name)
	if err != nil {
		return "", err
	}

	tempPath := f.Name()

	defer f.Close()

	totalWriteLen := int64(0)
	for totalWriteLen < size {
		writeLen, err := f.Write(dataBuf)
		if err != nil {
			os.Remove(tempPath)
			return "", err
		}

		totalWriteLen += int64(writeLen)
	}

	return tempPath, nil
}

func getHomeDir(testID string) string {
	account := GetTestAccount()
	return fmt.Sprintf("/%s/home/%s/%s", account.ClientZone, account.ClientUser, testID)
}

func makeHomeDir(t *testing.T, testID string) {
	account := GetTestAccount()
	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	assert.NoError(t, err)
	defer sess.Release()

	// first
	conn, err := sess.AcquireConnection()
	assert.NoError(t, err)

	homedir := getHomeDir(testID)
	err = fs.CreateCollection(conn, homedir, true)
	assert.NoError(t, err)
}
