// SPDX-License-Identifier: Apache-2.0
// Compatible with OpenZeppelin Contracts ^5.6.0
pragma solidity ^0.8.27;

import {
    AccessManagedUpgradeable
} from "@openzeppelin/contracts-upgradeable/access/manager/AccessManagedUpgradeable.sol";
import {ERC1155Upgradeable} from "@openzeppelin/contracts-upgradeable/token/ERC1155/ERC1155Upgradeable.sol";
import {
    ERC1155PausableUpgradeable
} from "@openzeppelin/contracts-upgradeable/token/ERC1155/extensions/ERC1155PausableUpgradeable.sol";
import {
    ERC1155SupplyUpgradeable
} from "@openzeppelin/contracts-upgradeable/token/ERC1155/extensions/ERC1155SupplyUpgradeable.sol";
import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

contract CollectibleCard is
    Initializable,
    ERC1155Upgradeable,
    AccessManagedUpgradeable,
    ERC1155PausableUpgradeable,
    ERC1155SupplyUpgradeable,
    ReentrancyGuardTransient,
    UUPSUpgradeable
{
    struct Card {
        string cardName;
        string description;
        uint32 amount;
        bool metadataFrozen;
    }

    uint256 private _cardId;
    address private _royaltyReceiver;
    string private _contractURI;
    uint256 private _totalOwedDividends;

    event CardCreated(uint256 indexed cardId, string cardName, uint256 originalPrice, uint32 amount);
    event MetadataFrozen(uint256 indexed cardId);
    event ModifyRoyalty(uint256 indexed cardId, uint16 royalty);
    event ModifyRoyaltyReceiver(address addr);
    event ContractURIUpdated();
    event DividendsDeposited(uint256 indexed tokenId, uint256 amount);
    event DividendsClaimed(address indexed user, uint256 indexed tokenId, uint256 amount);
    event FundsWithdrawn(address indexed admin, uint256 amount);

    mapping(uint256 => Card) public cards;
    mapping(uint256 => string) private _cardsURI;
    mapping(uint256 => uint16) private _royalties;
    mapping(uint256 => uint256) private _rewardPerTokenStored;
    mapping(address => mapping(uint256 => uint256)) private _userRewardPerTokenPaid;
    mapping(address => mapping(uint256 => uint256)) private _rewards;

    function createCard(
        address to,
        string calldata cardName,
        string calldata description,
        uint32 amount,
        string calldata tokenURI,
        uint256 price,
        uint16 royalty
    ) external restricted {
        require(royalty <= 10000, "Royalty exceeds 100%");

        uint256 _newId = _cardId;

        cards[_newId] = Card({cardName: cardName, description: description, amount: amount, metadataFrozen: false});
        _cardsURI[_newId] = tokenURI;
        _royalties[_newId] = royalty;

        _cardId++;

        _mint(to, _newId, amount, "");

        emit CardCreated(_newId, cardName, price, amount);
    }

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address initialAuthority, address royalRetriever, string calldata contractURI_)
        public
        initializer
    {
        require(royalRetriever != address(0), "Zero address");
        __ERC1155_init("");
        __AccessManaged_init(initialAuthority);
        __ERC1155Pausable_init();
        __ERC1155Supply_init();
        _royaltyReceiver = royalRetriever;
        _contractURI = contractURI_;
    }

    function contractURI() public view returns (string memory) {
        return _contractURI;
    }

    function setContractURI(string calldata newContractURI) external restricted {
        _contractURI = newContractURI;
        emit ContractURIUpdated();
    }

    function supportsInterface(bytes4 interfaceId) public view override returns (bool) {
        // 0x2a55205a = bytes4(keccak256("royaltyInfo(uint256,uint256)"))
        return interfaceId == 0x2a55205a || super.supportsInterface(interfaceId);
    }

    function _authorizeUpgrade(address newImplementation) internal override restricted {}

    function totalCards() public view returns (uint256) {
        return _cardId;
    }

    function royaltyReceiver() external view returns (address) {
        return _royaltyReceiver;
    }

    function modifyRoyaltyReceiver(address addr) external restricted {
        require(addr != address(0), "Zero address");
        _royaltyReceiver = addr;
        emit ModifyRoyaltyReceiver(addr);
    }

    function modifyRoyalty(uint256 tokenId, uint16 royalty) external restricted {
        require(tokenId < _cardId, "Card does not exist");
        require(royalty <= 10000, "Royalty exceeds 100%");
        _royalties[tokenId] = royalty;
        emit ModifyRoyalty(tokenId, royalty);
    }

    function royaltyInfo(uint256 _tokenId, uint256 _salePrice)
        external
        view
        returns (address receiver, uint256 royaltyAmount)
    {
        uint256 royalty = (_salePrice * _royalties[_tokenId]) / 10000;
        return (_royaltyReceiver, royalty);
    }

    function uri(uint256 tokenId) public view override returns (string memory) {
        if (tokenId >= _cardId) {
            return super.uri(tokenId);
        }

        string memory tokenURI = _cardsURI[tokenId];

        if (bytes(tokenURI).length > 0) {
            return tokenURI;
        }

        return super.uri(tokenId);
    }

    function setTokenURI(uint256 tokenId, string calldata newURI) external restricted {
        require(tokenId < _cardId, "Card does not exist");
        require(!cards[tokenId].metadataFrozen, "Metadata is frozen");
        _cardsURI[tokenId] = newURI;
        emit URI(newURI, tokenId);
    }

    function freezeMetadata(uint256 tokenId) external restricted {
        require(tokenId < _cardId, "Card does not exist");
        cards[tokenId].metadataFrozen = true;
        emit MetadataFrozen(tokenId);
    }

    function pause() public restricted {
        _pause();
    }

    function unpause() public restricted {
        _unpause();
    }

    function depositDividends(uint256 tokenId) external payable restricted {
        require(tokenId < _cardId, "Card does not exist");
        require(msg.value > 0, "No ETH sent");

        uint256 supply = totalSupply(tokenId);
        require(supply > 0, "No fractions in circulation");

        _rewardPerTokenStored[tokenId] += (msg.value * 1e18) / supply;
        _totalOwedDividends += msg.value;

        emit DividendsDeposited(tokenId, msg.value);
    }

    function earned(address user, uint256 tokenId) public view returns (uint256) {
        uint256 balance = balanceOf(user, tokenId);
        uint256 perToken = _rewardPerTokenStored[tokenId] - _userRewardPerTokenPaid[user][tokenId];
        return _rewards[user][tokenId] + (balance * perToken) / 1e18;
    }

    function claimDividends(uint256 tokenId) external nonReentrant {
        require(tokenId < _cardId, "Card does not exist");

        _updateReward(msg.sender, tokenId);

        uint256 reward = _rewards[msg.sender][tokenId];
        require(reward > 0, "No dividends to claim");

        // Checks-Effects-Interactions
        _rewards[msg.sender][tokenId] = 0;
        _totalOwedDividends -= reward;

        (bool success,) = payable(msg.sender).call{value: reward}("");
        require(success, "ETH transfer failed");

        emit DividendsClaimed(msg.sender, tokenId, reward);
    }

    function totalOwedDividends() external view returns (uint256) {
        return _totalOwedDividends;
    }

    function withdraw(uint256 amount) external restricted {
        uint256 available = address(this).balance - _totalOwedDividends;
        require(amount <= available, "Would withdraw owed dividends");

        (bool success,) = payable(msg.sender).call{value: amount}("");
        require(success, "ETH transfer failed");

        emit FundsWithdrawn(msg.sender, amount);
    }

    receive() external payable {}

    // ========== INTERNALS ==========

    function _updateReward(address user, uint256 tokenId) internal {
        if (user != address(0)) {
            _rewards[user][tokenId] = earned(user, tokenId);
            _userRewardPerTokenPaid[user][tokenId] = _rewardPerTokenStored[tokenId];
        }
    }

    function _update(address from, address to, uint256[] memory ids, uint256[] memory values)
        internal
        override(ERC1155Upgradeable, ERC1155PausableUpgradeable, ERC1155SupplyUpgradeable)
    {
        for (uint256 i = 0; i < ids.length; i++) {
            _updateReward(from, ids[i]);
            _updateReward(to, ids[i]);
        }

        super._update(from, to, ids, values);
    }

    uint256[49] private __gap;
}
