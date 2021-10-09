package aes

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestAes_CbcEncrypt(t *testing.T) {
	aes128, err := NewAes("GS317MrfMqnvFcEHIbPTuQ==", "1cijdjijnji89iju")
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("sdf312e3213")
	secretData := aes128.CbcEncrypt(data)
	plain, err := aes128.CbcDecrypt(secretData)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, plain) {
		t.Fatal("want true, got false")
	}

	t.Log(hex.EncodeToString(secretData))

	aes192, err := NewAes("DFSDFi8987ysdfasadf3a23f", "ji12jnchFgDejffe")
	secretData = aes192.CbcEncrypt(data)
	plain, err = aes192.CbcDecrypt(secretData)
	if !bytes.Equal(data, plain) {
		t.Fatal("want true, got false")
	}

	t.Log(hex.EncodeToString(secretData))

	aes256, err := NewAes("DFSDFi8987ysdfasadf3a23fJUDH7yuG", "uh12jnchFgDejffe")
	secretData = aes256.CbcEncrypt(data)
	plain, err = aes256.CbcDecrypt(secretData)
	if !bytes.Equal(data, plain) {
		t.Fatal("want true, got false")
	}

	t.Log(hex.EncodeToString(secretData))
}
