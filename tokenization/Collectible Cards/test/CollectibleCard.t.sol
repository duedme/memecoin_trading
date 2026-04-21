// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.27;

import {Test, console} from "forge-std/Test.sol";
import {AccessManager} from "@openzeppelin/contracts/access/manager/AccessManager.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {CollectibleCard} from "../src/CollectibleCard.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

contract CollectibleCardTest is Test {
    AccessManager public manager;
    CollectibleCard public card;

    address public admin = address(this);
    address public alice = makeAddr("alice");
    address public bob = makeAddr("bob");
    address public eve = makeAddr("eve"); // unauthorized

    uint64 public constant CARD_MANAGER_ROLE = 1;
    uint64 public constant DIVIDEND_MANAGER_ROLE = 2;
    uint64 public constant UPGRADER_ROLE = 3;

    function setUp() public {
        // 1. AccessManager
        manager = new AccessManager(admin);

        // 2. Implementation + Proxy
        CollectibleCard impl = new CollectibleCard();
        bytes memory initData = abi.encodeCall(
            CollectibleCard.initialize, (address(manager), admin, "https://example.com/collection.json")
        );
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        card = CollectibleCard(payable(address(proxy)));

        // 3. Configure roles
        bytes4[] memory cardSel = new bytes4[](9);
        cardSel[0] = CollectibleCard.createCard.selector;
        cardSel[1] = CollectibleCard.setTokenURI.selector;
        cardSel[2] = CollectibleCard.freezeMetadata.selector;
        cardSel[3] = CollectibleCard.setContractURI.selector;
        cardSel[4] = CollectibleCard.modifyRoyalty.selector;
        cardSel[5] = CollectibleCard.modifyRoyaltyReceiver.selector;
        cardSel[6] = CollectibleCard.pause.selector;
        cardSel[7] = CollectibleCard.unpause.selector;
        cardSel[8] = CollectibleCard.withdraw.selector;
        manager.setTargetFunctionRole(address(proxy), cardSel, CARD_MANAGER_ROLE);

        bytes4[] memory divSel = new bytes4[](1);
        divSel[0] = CollectibleCard.depositDividends.selector;
        manager.setTargetFunctionRole(address(proxy), divSel, DIVIDEND_MANAGER_ROLE);

        bytes4[] memory upgSel = new bytes4[](1);
        upgSel[0] = UUPSUpgradeable.upgradeToAndCall.selector;
        manager.setTargetFunctionRole(address(proxy), upgSel, UPGRADER_ROLE);

        manager.grantRole(CARD_MANAGER_ROLE, admin, 0);
        manager.grantRole(DIVIDEND_MANAGER_ROLE, admin, 0);
        manager.grantRole(UPGRADER_ROLE, admin, 0);
    }

    // ===== DEPLOYMENT =====

    function test_InitializeCorrectly() public view {
        assertEq(card.contractURI(), "https://example.com/collection.json");
        assertEq(card.totalCards(), 0);
    }

    function test_CannotInitializeTwice() public {
        vm.expectRevert();
        card.initialize(address(manager), admin, "hack");
    }

    // ===== CARD CREATION =====

    function test_CreateCard() public {
        card.createCard(alice, "Dragon", "Fire dragon", 100, "ipfs://dragon", 1 ether, 500);
        assertEq(card.totalCards(), 1);
        assertEq(card.balanceOf(alice, 0), 100);
    }

    function test_RevertCreateCard_Unauthorized() public {
        vm.prank(eve);
        vm.expectRevert();
        card.createCard(eve, "Hack", "No", 1, "ipfs://x", 0, 0);
    }

    function test_RevertCreateCard_RoyaltyTooHigh() public {
        vm.expectRevert("Royalty exceeds 100%");
        card.createCard(alice, "Bad", "Card", 10, "ipfs://x", 1 ether, 10001);
    }

    // ===== METADATA =====

    function test_SetTokenURI() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://old", 1 ether, 500);
        card.setTokenURI(0, "ipfs://new");
        assertEq(card.uri(0), "ipfs://new");
    }

    function test_FreezeBlocksURIChange() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        card.freezeMetadata(0);
        vm.expectRevert("Metadata is frozen");
        card.setTokenURI(0, "ipfs://hacked");
    }

    // ===== ROYALTIES =====

    function test_RoyaltyInfo() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        (address receiver, uint256 amount) = card.royaltyInfo(0, 10 ether);
        assertEq(receiver, admin);
        assertEq(amount, 0.5 ether); // 5% of 10 ETH
    }

    function test_SupportsEIP2981() public view {
        assertTrue(card.supportsInterface(0x2a55205a));
    }

    // ===== DIVIDENDOS =====

    function test_DepositAndEarn() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        card.depositDividends{value: 10 ether}(0);
        assertEq(card.earned(alice, 0), 10 ether);
    }

    function test_DividendsSplitProportionally() public {
        card.createCard(admin, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        card.safeTransferFrom(admin, alice, 0, 30, "");
        card.safeTransferFrom(admin, bob, 0, 20, "");

        card.depositDividends{value: 10 ether}(0);

        assertEq(card.earned(admin, 0), 5 ether); // 50%
        assertEq(card.earned(alice, 0), 3 ether); // 30%
        assertEq(card.earned(bob, 0), 2 ether); // 20%
    }

    function test_ClaimDividends() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        card.depositDividends{value: 10 ether}(0);

        uint256 before = alice.balance;
        vm.prank(alice);
        card.claimDividends(0);

        assertEq(alice.balance - before, 10 ether);
        assertEq(card.earned(alice, 0), 0);
    }

    function test_TransferPreservesRewards() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        card.depositDividends{value: 10 ether}(0);

        vm.prank(alice);
        card.safeTransferFrom(alice, bob, 0, 50, "");

        assertEq(card.earned(alice, 0), 10 ether); // ganó antes de transferir
        assertEq(card.earned(bob, 0), 0); // no tenía fracciones

        card.depositDividends{value: 10 ether}(0);
        assertEq(card.earned(alice, 0), 15 ether); // 10 + 5
        assertEq(card.earned(bob, 0), 5 ether); // 0 + 5
    }

    // ===== TOTAL OWED & WITHDRAW =====

    function test_WithdrawProtectsOwedFunds() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);

        (bool ok,) = address(card).call{value: 5 ether}("");
        assertTrue(ok);

        card.depositDividends{value: 10 ether}(0);

        card.withdraw(5 ether); // los 5 ETH libres

        vm.expectRevert("Would withdraw owed dividends");
        card.withdraw(1);
    }

    // ===== PAUSABLE =====

    function test_PauseBlocksTransfers() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        card.pause();

        vm.prank(alice);
        vm.expectRevert();
        card.safeTransferFrom(alice, bob, 0, 10, "");
    }

    // ===== ACCESS CONTROL =====

    function test_UnauthorizedCannotDeposit() public {
        card.createCard(alice, "Dragon", "Fire", 100, "ipfs://x", 1 ether, 500);
        vm.deal(eve, 10 ether);
        vm.prank(eve);
        vm.expectRevert();
        card.depositDividends{value: 1 ether}(0);
    }

    function onERC1155Received(address, address, uint256, uint256, bytes memory) external pure returns (bytes4) {
        return this.onERC1155Received.selector;
    }

    function onERC1155BatchReceived(address, address, uint256[] memory, uint256[] memory, bytes memory)
        external
        pure
        returns (bytes4)
    {
        return this.onERC1155BatchReceived.selector;
    }

    receive() external payable {}
}
