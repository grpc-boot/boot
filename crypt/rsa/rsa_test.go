package rsa

import (
	"crypto"
	"encoding/hex"
	"testing"
)

func TestCreateKeys(t *testing.T) {
	privateKey, publicKey := CreateKeys(2048)
	t.Logf("privateKey:%s, publicKey:%s", privateKey, publicKey)
}

func TestCreatePkcs8Keys(t *testing.T) {
	privateKey, publicKey := CreatePkcs8Keys(2048)
	t.Logf("privateKey:%s, publicKey:%s", privateKey, publicKey)
}

func TestRsa_Decrypt(t *testing.T) {
	privateKey, publicKey := CreateKeys(2048)
	rsa, _ := NewRsa(publicKey, privateKey)

	data := []byte("sadfasfd")
	secretData, err := rsa.Encrypt(data)
	if err != nil {
		t.Fatal(err)
	}
	pData, err := rsa.Decrypt(secretData)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("data:%s, secretData:%s, decryptData:%s", string(data), hex.EncodeToString(secretData), string(pData))

	privateKey, publicKey = CreatePkcs8Keys(2048)
	rsa, _ = NewRsa(publicKey, privateKey)

	data = []byte("sadfaasdfasdfasdfsfd")
	secretData, err = rsa.Encrypt(data)
	if err != nil {
		t.Fatal(err)
	}
	pData, err = rsa.Decrypt(secretData)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("data:%s, secretData:%s, decryptData:%s", string(data), hex.EncodeToString(secretData), string(pData))
}

func TestRsa_Verify(t *testing.T) {
	privateKey, publicKey := CreateKeys(2048)
	rsa, _ := NewRsa(publicKey, privateKey)
	data := []byte("sadfaasdfasdfasdfsfd")
	sign, err := rsa.Sign(data, crypto.SHA1)
	if err != nil {
		t.Fatal(err)
	}

	if !rsa.Verify(data, sign, crypto.SHA1) {
		t.Fatal("want true got false")
	}
}
