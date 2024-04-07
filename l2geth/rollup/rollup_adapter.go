package rollup

import (
	"errors"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum-optimism/optimism/l2geth/accounts/abi"
	"github.com/ethereum-optimism/optimism/l2geth/accounts/abi/bind"
	"github.com/ethereum-optimism/optimism/l2geth/contracts/checkpointoracle/contract/seqset"
	"github.com/ethereum-optimism/optimism/l2geth/core"
	"github.com/ethereum-optimism/optimism/l2geth/crypto"

	"github.com/ethereum-optimism/optimism/l2geth/common"
	"github.com/ethereum-optimism/optimism/l2geth/core/types"
	"github.com/ethereum-optimism/optimism/l2geth/log"
	"github.com/ethereum-optimism/optimism/l2geth/rollup/rcfg"
)

// RollupAdapter is the adapter for decentralized sequencers
// that is required by the SyncService
type RollupAdapter interface {
	RecoverSeqAddress(tx *types.Transaction) (common.Address, error)
	// get tx sequencer for checking tx is valid
	GetTxSequencer(tx *types.Transaction, expectIndex uint64) (common.Address, error)
	// check current sequencer is working
	// CheckSequencerIsWorking() bool
	//
	GetSeqValidHeight() uint64
	GetFinalizedBlock() (uint64, error)
	ParseUpdateSeqData(data []byte) *RecommitEpoch
	IsSeqSetContractCall(tx *types.Transaction) (bool, []byte)
	IsRespanCall(tx *types.Transaction) bool
	IsPreRespanSequencer(seqAddress common.Address, number uint64) bool
	IsNotNextRespanSequencer(seqAddress common.Address, number uint64) bool
}

// Cached seq epoch, if recommit or block number < start | > end, clear cache with status false
type CachedSeqEpoch struct {
	Signer     common.Address
	StartBlock *big.Int
	EndBlock   *big.Int
	RespanArr  []*big.Int
	Status     bool
}

// RreRespan is called by bridge, to notify sequencer to prevent save p2p blocks >= RespanStartBlock
type PreRespan struct {
	PreSigner        common.Address
	NewSigner        common.Address
	RespanStartBlock uint64
}

// SeqAdapter is an adapter used by sequencer based RollupClient
type SeqAdapter struct {
	cachedSeqEpoch *CachedSeqEpoch
	preRespan      *PreRespan

	chain    *core.BlockChain
	contract *seqset.Seqset
	abi      abi.ABI

	Address     common.Address
	ValidHeight uint64
}

func NewSeqAdapter(bc *core.BlockChain, client bind.ContractBackend) (*SeqAdapter, error) {
	seqsetAbi, err := abi.JSON(strings.NewReader(seqset.SeqsetABI))
	if err != nil {
		return nil, err
	}

	chainConfig := bc.Config().MetisRollupConfig()

	seqsetContract, err := seqset.NewSeqset(chainConfig.SeqSetContract, client)
	if err != nil {
		return nil, err
	}

	return &SeqAdapter{
		chain:       bc,
		contract:    seqsetContract,
		abi:         seqsetAbi,
		Address:     chainConfig.SeqSetContract,
		ValidHeight: chainConfig.SeqSetHeight.Uint64(),
	}, nil
}

type RecommitEpoch struct {
	OldEpochId *big.Int
	NewEpochId *big.Int
	StartBlock *big.Int
	EndBlock   *big.Int
	NewSigner  common.Address
}

func (s *SeqAdapter) ParseUpdateSeqData(data []byte) *RecommitEpoch {
	method, ok := s.abi.Methods["recommitEpoch"]
	if !ok {
		panic("no recommitEpoch found")
	}

	if len(data) < 4 {
		return nil
	}

	var recommit RecommitEpoch
	if err := method.Inputs.Unpack(&recommit, data[4:]); err != nil {
		return nil
	}
	return &recommit
}

func (s *SeqAdapter) insertRespanNumber(arr []*big.Int, value *big.Int) []*big.Int {
	index := sort.Search(len(arr), func(i int) bool {
		return arr[i].Cmp(value) >= 0
	})

	arr = append(arr, nil)
	if index < len(arr)-1 {
		copy(arr[index+1:], arr[index:])
	}
	arr[index] = new(big.Int).Set(value)

	return arr
}

func (s *SeqAdapter) removeFirstRespan(arr []*big.Int) []*big.Int {
	if len(arr) > 0 {
		index := 0
		copy(arr[index:], arr[index+1:])
		arr = arr[:len(arr)-1]
	}

	return arr
}

func (s *SeqAdapter) getSequencer(expectIndex uint64) (common.Address, error) {
	if s.cachedSeqEpoch.Status && (s.cachedSeqEpoch.StartBlock.Uint64() > expectIndex || s.cachedSeqEpoch.EndBlock.Uint64() < expectIndex) {
		s.cachedSeqEpoch.Status = false
	}

	blockNumber := s.chain.CurrentBlock().NumberU64()

	// clear preRespan when block >= respanStart
	if s.preRespan != nil && s.preRespan.RespanStartBlock <= blockNumber {
		log.Info("clear pre respan")
		s.preRespan = nil
	}
	if len(s.cachedSeqEpoch.RespanArr) > 0 {
		// at this time, blockChain has not reach the respan height, pause sequencer with an error
		respanStart := s.cachedSeqEpoch.RespanArr[0].Uint64()
		if expectIndex > respanStart && blockNumber < respanStart {
			log.Error("Get sequencer error when check respan", "expectIndex", expectIndex, "blockNumber", blockNumber, "respanStart", respanStart)
			return common.Address{}, errors.New("get sequencer error when check respan")
		}
		if blockNumber >= respanStart {
			// remove [0], reload cache
			s.cachedSeqEpoch.RespanArr = s.removeFirstRespan(s.cachedSeqEpoch.RespanArr)
			s.cachedSeqEpoch.Status = false
		}
	}
	if !s.cachedSeqEpoch.Status {
		// start loading cache
		log.Info("get tx seqeuencer start epoch cache")
		// re-cache the epoch info
		epochNumber, err := s.contract.GetEpochByBlock(nil, big.NewInt(int64(expectIndex)))
		if err != nil {
			log.Error("Get sequencer error when GetEpochByBlock", "err", err)
			return common.Address{}, err
		}
		currentEpochNumber, err := s.contract.CurrentEpochNumber(nil)
		if err != nil {
			log.Error("Get sequencer error when CurrentEpochNumber", "err", err)
			return common.Address{}, err
		}
		if epochNumber.Uint64() > currentEpochNumber.Uint64() {
			log.Error("Get sequencer incorrect epoch number", "epoch", epochNumber.Uint64(), "current", currentEpochNumber.Uint64())
			return common.Address{}, errors.New("get sequencer incorrect epoch number")
		}
		epoch, err := s.contract.Epochs(nil, epochNumber)
		if err != nil {
			log.Error("Get sequencer error when Epochs", "err", err)
			return common.Address{}, err
		}
		s.cachedSeqEpoch.Signer = epoch.Signer
		s.cachedSeqEpoch.StartBlock = epoch.StartBlock
		s.cachedSeqEpoch.EndBlock = epoch.EndBlock
		s.cachedSeqEpoch.Status = true
		// loaded epoch cache
		log.Info("get tx seqeuencer loaded epoch cache", "status", s.cachedSeqEpoch.Status, "start", s.cachedSeqEpoch.StartBlock.Uint64(), "end", s.cachedSeqEpoch.EndBlock.Uint64(), "signer", s.cachedSeqEpoch.Signer.String())
	}
	return s.cachedSeqEpoch.Signer, nil
}

func (s *SeqAdapter) GetSeqValidHeight() uint64 {
	return s.ValidHeight
}

func (s *SeqAdapter) GetFinalizedBlock() (uint64, error) {
	blockNumber := uint64(0)
	block := s.chain.CurrentBlock()
	if block != nil {
		blockNumber = block.Number().Uint64()
	}
	if blockNumber < s.ValidHeight {
		return blockNumber, nil
	}
	var err error
	finalizedBlock, err := s.contract.FinalizedBlock(nil)
	if err != nil {
		return 0, err
	}
	return finalizedBlock.Uint64(), nil
}

func (s *SeqAdapter) RecoverSeqAddress(tx *types.Transaction) (common.Address, error) {
	// enqueue tx no sign
	if tx.QueueOrigin() == types.QueueOriginL1ToL2 {
		return common.Address{}, errors.New("enqueue seq sign is null")
	}
	seqSign := tx.GetSeqSign()
	if seqSign == nil {
		return common.Address{}, errors.New("seq sign is null")
	}

	var signBytes []byte
	signBytes = append(signBytes, seqSign.R.FillBytes(make([]byte, 32))...)
	signBytes = append(signBytes, seqSign.S.FillBytes(make([]byte, 32))...)
	signBytes = append(signBytes, byte(seqSign.V.Int64()))

	signer, err := crypto.SigToPub(tx.Hash().Bytes(), signBytes)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(*signer), nil
}

func (s *SeqAdapter) IsSeqSetContractCall(tx *types.Transaction) (bool, []byte) {
	toAddress := tx.To()
	return !s.Address.IsZero() && toAddress != nil && *toAddress == s.Address, tx.Data()
}

func (s *SeqAdapter) IsRespanCall(tx *types.Transaction) bool {
	if tx == nil {
		return false
	}
	seqOper, data := s.IsSeqSetContractCall(tx)
	if !seqOper {
		return false
	}
	return s.ParseUpdateSeqData(data) != nil
}

func (s *SeqAdapter) IsPreRespanSequencer(seqAddress common.Address, number uint64) bool {
	if s.preRespan == nil || s.preRespan.RespanStartBlock == 0 || (s.preRespan.PreSigner == common.Address{}) {
		return false
	}
	return number >= s.preRespan.RespanStartBlock && s.preRespan.PreSigner == seqAddress
}

func (s *SeqAdapter) IsNotNextRespanSequencer(seqAddress common.Address, number uint64) bool {
	if s.preRespan == nil || s.preRespan.RespanStartBlock == 0 || (s.preRespan.NewSigner == common.Address{}) {
		return false
	}
	return number >= s.preRespan.RespanStartBlock && s.preRespan.NewSigner != seqAddress
}

func (s *SeqAdapter) GetTxSequencer(tx *types.Transaction, expectIndex uint64) (common.Address, error) {
	// check is update sequencer operate
	if expectIndex <= s.ValidHeight {
		// return default address
		return rcfg.DefaultSeqAdderss, nil
	}

	if tx != nil {
		// seqOper, data := s.IsSeqSetContractCall(tx)
		// if seqOper {
		// 	updateSeq, newSeq, startBlock, endBlock := s.ParseUpdateSeqData(data)
		// 	if updateSeq {
		// 		// respan
		// 		log.Info("get tx seqeuencer respan", "respan-start", startBlock.Uint64(), "respan-end", endBlock.Uint64(), "respan-signer", newSeq.String())
		// 		// cache the respan start block
		// 		s.cachedSeqEpoch.RespanArr = s.insertRespanNumber(s.cachedSeqEpoch.RespanArr, startBlock)
		// 		// return the respan sequencer, it will mine. it can success or fail
		// 		return newSeq, nil
		// 	}
		// }
	}

	// log.Debug("Will get sequencer info from seq contract on L2")
	// get status from contract on height expectIndex - 1
	// return result ,err
	address, err := s.getSequencer(expectIndex)
	log.Debug("GetTxSequencer ", "getSequencer address", address, "expectIndex", expectIndex, "contract address ", s.Address, "err", err)

	// a special case, when error with "get sequencer incorrect epoch number",
	// allows transfers to mpcAddress, and the block is generated by the sequencer of the previous epoch.
	if tx != nil && err != nil && err.Error() == "get sequencer incorrect epoch number" {
		// get latest sequencer
		currentEpochNumber, err2 := s.contract.CurrentEpochNumber(nil)
		if err2 != nil {
			log.Error("Get sequencer error when CurrentEpochNumber 2", "err", err2)
			return address, err
		}
		epoch, err2 := s.contract.Epochs(nil, currentEpochNumber)
		if err2 != nil {
			log.Error("Get sequencer error when Epochs 2", "err", err2)
			return address, err
		}
		return epoch.Signer, nil
	}

	return address, err
}
