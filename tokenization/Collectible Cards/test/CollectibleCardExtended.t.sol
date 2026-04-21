// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.27;

import {Test, console} from "forge-std/Test.sol";
import {AccessManager} from "@openzeppelin/contracts/access/manager/AccessManager.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {IERC1155Receiver} from "@openzeppelin/contracts/token/ERC1155/IERC1155Receiver.sol";
import {CollectibleCard} from "../src/CollectibleCard.sol";

// ========== CONTRATOS AUXILIARES PARA TESTS ==========

/// @dev Contrato malicioso que intenta reentrancy en claimDividends
/// Implementa IERC1155Receiver para poder recibir ERC1155 tokens
contract ReentrancyAttacker is IERC1155Receiver {
    CollectibleCard public target;
    uint256 public tokenId;
    uint256 public attackCount;

    constructor(address _target, uint256 _tokenId) {
        target = CollectibleCard(payable(_target));
        tokenId = _tokenId;
    }

    function attack() external {
        target.claimDividends(tokenId);
    }

    receive() external payable {
        if (attackCount < 2) {
            attackCount++;
            target.claimDividends(tokenId);
        }
    }

    function onERC1155Received(address, address, uint256, uint256, bytes calldata)
        external pure override returns (bytes4)
    {
        return this.onERC1155Received.selector;
    }

    function onERC1155BatchReceived(address, address, uint256[] calldata, uint256[] calldata, bytes calldata)
        external pure override returns (bytes4)
    {
        return this.onERC1155BatchReceived.selector;
    }

    function supportsInterface(bytes4 interfaceId) external pure override returns (bool) {
        return interfaceId == type(IERC1155Receiver).interfaceId;
    }
}

/// @dev Contrato que puede recibir ERC1155 pero rechaza ETH
contract ETHRejecter is IERC1155Receiver {
    function onERC1155Received(address, address, uint256, uint256, bytes calldata)
        external pure override returns (bytes4)
    {
        return this.onERC1155Received.selector;
    }

    function onERC1155BatchReceived(address, address, uint256[] calldata, uint256[] calldata, bytes calldata)
        external pure override returns (bytes4)
    {
        return this.onERC1155BatchReceived.selector;
    }

    function supportsInterface(bytes4 interfaceId) external pure override returns (bool) {
        return interfaceId == type(IERC1155Receiver).interfaceId;
    }

    // No tiene receive() ni fallback() — acepta ERC1155 pero rechaza ETH
}

// ========== TEST PRINCIPAL ==========
contract CollectibleCardExtendedTest is Test {
    CollectibleCard public card;
    AccessManager public manager;
    ERC1967Proxy public proxy;

    address public alice = makeAddr("alice");
    address public bob = makeAddr("bob");
    address public charlie = makeAddr("charlie");
    address public eve = makeAddr("eve");

    function setUp() public {
        manager = new AccessManager(address(this));
        CollectibleCard impl = new CollectibleCard();
        proxy = new ERC1967Proxy(
            address(impl),
            abi.encodeCall(CollectibleCard.initialize, (address(manager), address(this), "https://example.com/collection.json"))
        );
        card = CollectibleCard(payable(address(proxy)));
    }

    // ───────────── helpers ─────────────
    function _createCard(address to, uint32 amount, uint256 price, uint16 royalty) internal returns (uint256) {
        uint256 id = card.totalCards();
        card.createCard(to, "Card", "Desc", amount, "ipfs://meta", price, royalty);
        return id;
    }

    function _createAndFund(address to, uint32 amount, uint256 depositAmt) internal returns (uint256) {
        uint256 id = _createCard(to, amount, 1 ether, 500);
        card.depositDividends{value: depositAmt}(id);
        return id;
    }

    // ══════════════════════════════════════════════
    //  NIVEL 1 — TESTS UNITARIOS
    // ══════════════════════════════════════════════

    function test_ReentrancyAttackOnClaimFails() public {
        uint256 id = card.totalCards();
        card.createCard(address(this), "Card", "D", 100, "ipfs://x", 1 ether, 500);

        ReentrancyAttacker attacker = new ReentrancyAttacker(address(card), id);
        card.safeTransferFrom(address(this), address(attacker), id, 50, "");

        card.depositDividends{value: 10 ether}(id);

        vm.expectRevert();
        attacker.attack();
    }

    function test_CannotClaimWithZeroBalance() public {
        _createCard(alice, 100, 1 ether, 500);
        vm.prank(bob);
        vm.expectRevert("No dividends to claim");
        card.claimDividends(0);
    }

    function test_CannotClaimWhenNoDividendsDeposited() public {
        _createCard(alice, 100, 1 ether, 500);
        vm.prank(alice);
        vm.expectRevert("No dividends to claim");
        card.claimDividends(0);
    }

    function test_CannotDepositToNonexistentCard() public {
        vm.expectRevert("Card does not exist");
        card.depositDividends{value: 1 ether}(999);
    }

    function test_CannotDepositZeroETH() public {
        _createCard(alice, 100, 1 ether, 500);
        vm.expectRevert("No ETH sent");
        card.depositDividends{value: 0}(0);
    }

    function test_DividendsIsolatedPerCard() public {
        uint256 card0 = _createCard(alice, 100, 1 ether, 500);
        uint256 card1 = _createCard(bob, 50, 2 ether, 300);

        card.depositDividends{value: 10 ether}(card0);
        card.depositDividends{value: 5 ether}(card1);

        assertEq(card.earned(alice, card0), 10 ether);
        assertEq(card.earned(alice, card1), 0);
        assertEq(card.earned(bob, card1), 5 ether);
        assertEq(card.earned(bob, card0), 0);
    }

    function test_ModifyRoyalty() public {
        _createCard(alice, 100, 1 ether, 500);
        card.modifyRoyalty(0, 1000);
        (address receiver, uint256 royalty) = card.royaltyInfo(0, 10 ether);
        assertEq(receiver, address(this));
        assertEq(royalty, 1 ether);
    }

    function test_CannotModifyRoyaltyAboveMax() public {
        _createCard(alice, 100, 1 ether, 500);
        vm.expectRevert("Royalty exceeds 100%");
        card.modifyRoyalty(0, 10001);
    }

    function test_CannotModifyRoyaltyNonexistentCard() public {
        vm.expectRevert("Card does not exist");
        card.modifyRoyalty(999, 500);
    }

    function test_ModifyRoyaltyReceiver() public {
        card.modifyRoyaltyReceiver(alice);
        assertEq(card.royaltyReceiver(), alice);
    }

    function test_RoyaltyReceiverCannotBeSetToZero() public {
        vm.expectRevert("Zero address");
        card.modifyRoyaltyReceiver(address(0));
    }

    function test_SetContractURI() public {
        card.setContractURI("ipfs://new-contract");
        assertEq(card.contractURI(), "ipfs://new-contract");
    }

    function test_UnpauseRestoresTransfers() public {
        _createCard(alice, 100, 1 ether, 500);
        card.pause();
        card.unpause();
        vm.prank(alice);
        card.safeTransferFrom(alice, bob, 0, 10, "");
        assertEq(card.balanceOf(bob, 0), 10);
    }

    function test_CannotSetURINonexistentCard() public {
        vm.expectRevert("Card does not exist");
        card.setTokenURI(999, "ipfs://hack");
    }

    function test_CannotFreezeNonexistentCard() public {
        vm.expectRevert("Card does not exist");
        card.freezeMetadata(999);
    }

    function test_CannotClaimNonexistentCard() public {
        vm.expectRevert("Card does not exist");
        card.claimDividends(999);
    }

    function test_UriOfNonexistentCardReturnsBaseURI() public view {
        string memory u = card.uri(999);
        assertEq(u, "");
    }

    function test_ForceFeedETHDoesNotBreakWithdraw() public {
        _createCard(alice, 100, 1 ether, 500);
        card.depositDividends{value: 5 ether}(0);
        vm.deal(address(card), address(card).balance + 3 ether);
        card.withdraw(3 ether);
        vm.expectRevert("Would withdraw owed dividends");
        card.withdraw(1);
    }

    function test_WithdrawZero() public {
        (bool ok,) = address(card).call{value: 1 ether}("");
        require(ok);
        card.withdraw(0);
    }

    function test_UnauthorizedCannotWithdraw() public {
        (bool ok,) = address(card).call{value: 1 ether}("");
        require(ok);
        vm.prank(eve);
        vm.expectRevert();
        card.withdraw(1 ether);
    }

    function test_ClaimFailsWhenReceiverRejectsETH() public {
        ETHRejecter rejecter = new ETHRejecter();
        uint256 id = card.totalCards();
        card.createCard(address(this), "Card", "D", 100, "ipfs://x", 1 ether, 500);
        card.safeTransferFrom(address(this), address(rejecter), id, 100, "");

        card.depositDividends{value: 5 ether}(id);

        vm.prank(address(rejecter));
        vm.expectRevert("ETH transfer failed");
        card.claimDividends(id);
    }

    function test_DoubleClaim() public {
        uint256 id = _createAndFund(alice, 100, 10 ether);

        vm.startPrank(alice);
        card.claimDividends(id);
        vm.expectRevert("No dividends to claim");
        card.claimDividends(id);
        vm.stopPrank();
    }

    function test_MultipleDividendDeposits() public {
        uint256 id = _createCard(alice, 100, 1 ether, 500);
        card.depositDividends{value: 3 ether}(id);
        card.depositDividends{value: 7 ether}(id);
        assertEq(card.earned(alice, id), 10 ether);
    }

    function test_BatchTransferUpdatesRewards() public {
        uint256 id0 = _createCard(address(this), 100, 1 ether, 500);
        uint256 id1 = _createCard(address(this), 50, 2 ether, 300);

        card.depositDividends{value: 10 ether}(id0);
        card.depositDividends{value: 5 ether}(id1);

        uint256[] memory ids = new uint256[](2);
        ids[0] = id0;
        ids[1] = id1;
        uint256[] memory amounts = new uint256[](2);
        amounts[0] = 30;
        amounts[1] = 20;

        card.safeBatchTransferFrom(address(this), alice, ids, amounts, "");

        assertEq(card.earned(address(this), id0), 10 ether);
        assertEq(card.earned(address(this), id1), 5 ether);
        assertEq(card.earned(alice, id0), 0);
        assertEq(card.earned(alice, id1), 0);

        card.depositDividends{value: 10 ether}(id0);
        assertEq(card.earned(alice, id0), 3 ether);
    }

    function test_CreateCardZeroAmount() public {
        card.createCard(alice, "Empty", "D", 0, "ipfs://x", 1 ether, 500);
        assertEq(card.balanceOf(alice, 0), 0);
    }

    function test_CannotDepositToZeroSupplyCard() public {
        card.createCard(alice, "Empty", "D", 0, "ipfs://x", 1 ether, 500);
        vm.expectRevert("No fractions in circulation");
        card.depositDividends{value: 1 ether}(0);
    }

    function test_SupportsERC1155Interface() public view {
        assertTrue(card.supportsInterface(0xd9b67a26));
        assertTrue(card.supportsInterface(0x0e89341c));
        assertTrue(card.supportsInterface(0x01ffc9a7));
        assertTrue(card.supportsInterface(0x2a55205a));
    }

    function test_TotalOwedReducesOnClaim() public {
        uint256 id = _createAndFund(alice, 100, 10 ether);
        assertEq(card.totalOwedDividends(), 10 ether);

        vm.prank(alice);
        card.claimDividends(id);
        assertEq(card.totalOwedDividends(), 0);
    }

    function test_ZeroRoyalty() public {
        _createCard(alice, 100, 1 ether, 0);
        (, uint256 royalty) = card.royaltyInfo(0, 10 ether);
        assertEq(royalty, 0);
    }

    function test_MaxRoyalty() public {
        _createCard(alice, 100, 1 ether, 10000);
        (, uint256 royalty) = card.royaltyInfo(0, 10 ether);
        assertEq(royalty, 10 ether);
    }

    function test_SelfTransferPreservesRewards() public {
        uint256 id = _createAndFund(alice, 100, 10 ether);
        vm.prank(alice);
        card.safeTransferFrom(alice, alice, id, 50, "");
        assertEq(card.earned(alice, id), 10 ether);
    }

    // ══════════════════════════════════════════════
    //  NIVEL 2 — FUZZ TESTS
    // ══════════════════════════════════════════════

    function testFuzz_DividendsNeverExceedDeposit(
        uint32 aliceAmt,
        uint32 bobAmt,
        uint96 depositAmt
    ) public {
        vm.assume(aliceAmt > 0 && aliceAmt <= 10000);
        vm.assume(bobAmt > 0 && bobAmt <= 10000);
        vm.assume(depositAmt > 0 && depositAmt <= 1000 ether);

        uint32 total = aliceAmt + bobAmt;
        vm.assume(total >= aliceAmt);

        uint256 id = card.totalCards();
        card.createCard(address(this), "Card", "D", total, "ipfs://x", 1 ether, 500);
        card.safeTransferFrom(address(this), alice, id, aliceAmt, "");
        card.safeTransferFrom(address(this), bob, id, bobAmt, "");

        vm.deal(address(this), uint256(depositAmt) + 1 ether);
        card.depositDividends{value: depositAmt}(id);

        uint256 aliceEarned = card.earned(alice, id);
        uint256 bobEarned = card.earned(bob, id);
        uint256 thisEarned = card.earned(address(this), id);

        assertLe(aliceEarned + bobEarned + thisEarned, uint256(depositAmt));
    }

    function testFuzz_ClaimGetsCorrectAmount(uint96 depositAmt) public {
        vm.assume(depositAmt > 0 && depositAmt <= 1000 ether);

        uint256 id = _createCard(alice, 100, 1 ether, 500);
        vm.deal(address(this), uint256(depositAmt) + 1 ether);
        card.depositDividends{value: depositAmt}(id);

        uint256 earned = card.earned(alice, id);
        uint256 balBefore = alice.balance;

        vm.prank(alice);
        card.claimDividends(id);

        assertEq(alice.balance - balBefore, earned);
        assertEq(card.earned(alice, id), 0);
    }

    function testFuzz_RoyaltyCalculation(uint16 royaltyBps, uint128 salePrice) public {
        vm.assume(royaltyBps <= 10000);
        vm.assume(salePrice > 0);

        _createCard(alice, 100, 1 ether, royaltyBps);
        (, uint256 royalty) = card.royaltyInfo(0, salePrice);

        uint256 expected = (uint256(salePrice) * royaltyBps) / 10000;
        assertEq(royalty, expected);
    }

    function testFuzz_TransferPreservesExistingRewards(
        uint32 transferAmt,
        uint96 deposit1,
        uint96 deposit2
    ) public {
        vm.assume(transferAmt > 0 && transferAmt < 100);
        vm.assume(deposit1 > 0 && deposit1 <= 500 ether);
        vm.assume(deposit2 > 0 && deposit2 <= 500 ether);

        uint256 id = _createCard(alice, 100, 1 ether, 500);

        vm.deal(address(this), uint256(deposit1) + uint256(deposit2) + 1 ether);
        card.depositDividends{value: deposit1}(id);

        uint256 earnedBefore = card.earned(alice, id);

        vm.prank(alice);
        card.safeTransferFrom(alice, bob, id, transferAmt, "");

        card.depositDividends{value: deposit2}(id);

        assertGe(card.earned(alice, id), earnedBefore);
    }

    function testFuzz_WithdrawRespectsOwedDividends(uint96 extraETH, uint96 depositAmt) public {
        vm.assume(depositAmt > 0 && depositAmt <= 500 ether);
        vm.assume(extraETH > 0 && extraETH <= 500 ether);

        uint256 id = _createCard(alice, 100, 1 ether, 500);

        (bool ok,) = address(card).call{value: extraETH}("");
        require(ok);
        vm.deal(address(this), uint256(depositAmt) + 1 ether);
        card.depositDividends{value: depositAmt}(id);

        card.withdraw(uint256(extraETH));

        if (depositAmt > 0) {
            vm.expectRevert("Would withdraw owed dividends");
            card.withdraw(1);
        }
    }

    // ══════════════════════════════════════════════
    //  NIVEL 3 — EDGE CASES & SEGURIDAD AVANZADA
    // ══════════════════════════════════════════════

    function test_DustDividendsDontRevert() public {
        _createCard(alice, 100, 1 ether, 500);
        card.depositDividends{value: 99}(0);
        uint256 earned = card.earned(alice, 0);
        assertLe(earned, 99);
    }

    function test_ManyHolders() public {
        uint256 id = card.totalCards();
        card.createCard(address(this), "Card", "D", 100, "ipfs://x", 1 ether, 500);

        address[5] memory holders;
        uint256[5] memory amounts = [uint256(10), 20, 30, 15, 25];

        for (uint256 i = 0; i < 5; i++) {
            holders[i] = makeAddr(string(abi.encodePacked("holder", i)));
            card.safeTransferFrom(address(this), holders[i], id, amounts[i], "");
        }

        card.depositDividends{value: 100 ether}(id);

        for (uint256 i = 0; i < 5; i++) {
            uint256 expected = (100 ether * amounts[i]) / 100;
            assertEq(card.earned(holders[i], id), expected);
        }
    }

    function test_FullLifecycle() public {
        uint256 id = _createAndFund(alice, 100, 10 ether);

        vm.prank(alice);
        card.claimDividends(id);
        assertEq(alice.balance, 10 ether);

        vm.prank(alice);
        card.safeTransferFrom(alice, bob, id, 50, "");

        card.depositDividends{value: 20 ether}(id);

        vm.prank(alice);
        card.claimDividends(id);
        vm.prank(bob);
        card.claimDividends(id);

        assertEq(alice.balance, 20 ether);
        assertEq(bob.balance, 10 ether);
    }

    function test_ClaimWorksWhilePaused() public {
        uint256 id = _createAndFund(alice, 100, 10 ether);
        card.pause();
        vm.prank(alice);
        card.claimDividends(id);
        assertEq(alice.balance, 10 ether);
    }

    function test_PauseBlocksCreateCard() public {
        card.pause();
        vm.expectRevert();
        card.createCard(alice, "X", "D", 10, "ipfs://x", 1 ether, 500);
    }

    function test_CreateManyCards() public {
        for (uint256 i = 0; i < 20; i++) {
            card.createCard(alice, "Card", "D", 100, "ipfs://x", 1 ether, 500);
        }
        assertEq(card.totalCards(), 20);
        assertEq(card.balanceOf(alice, 19), 100);
    }

    function test_EarnedIsView() public {
        uint256 id = _createAndFund(alice, 100, 10 ether);
        uint256 e1 = card.earned(alice, id);
        uint256 e2 = card.earned(alice, id);
        assertEq(e1, e2);
    }

    function test_DepositAfterPartialClaim() public {
        uint256 id = _createCard(address(this), 100, 1 ether, 500);
        card.safeTransferFrom(address(this), alice, id, 60, "");
        card.safeTransferFrom(address(this), bob, id, 40, "");

        card.depositDividends{value: 10 ether}(id);

        vm.prank(alice);
        card.claimDividends(id);
        assertEq(alice.balance, 6 ether);

        card.depositDividends{value: 10 ether}(id);

        vm.prank(bob);
        card.claimDividends(id);
        assertEq(bob.balance, 8 ether);

        vm.prank(alice);
        card.claimDividends(id);
        assertEq(alice.balance, 12 ether);
    }

    function test_WithdrawSendsToCallerOnly() public {
        (bool ok,) = address(card).call{value: 5 ether}("");
        require(ok);
        uint256 balBefore = address(this).balance;
        card.withdraw(5 ether);
        assertEq(address(this).balance - balBefore, 5 ether);
    }

    function test_Invariant_BalanceGeqOwed() public {
        uint256 id = _createCard(address(this), 100, 1 ether, 500);
        (bool ok,) = address(card).call{value: 3 ether}("");
        require(ok);
        card.depositDividends{value: 10 ether}(id);
        card.safeTransferFrom(address(this), alice, id, 50, "");
        assertGe(address(card).balance, card.totalOwedDividends());
        card.withdraw(3 ether);
        assertGe(address(card).balance, card.totalOwedDividends());
    }

    function test_Invariant_SupplyConstant() public {
        _createCard(alice, 100, 1 ether, 500);
        uint256 supplyBefore = card.totalSupply(0);
        vm.prank(alice);
        card.safeTransferFrom(alice, bob, 0, 30, "");
        assertEq(card.totalSupply(0), supplyBefore);
    }

    // ───────────── ERC1155Receiver ─────────────
    function onERC1155Received(address, address, uint256, uint256, bytes memory)
        public pure returns (bytes4)
    {
        return this.onERC1155Received.selector;
    }

    function onERC1155BatchReceived(address, address, uint256[] memory, uint256[] memory, bytes memory)
        public pure returns (bytes4)
    {
        return this.onERC1155BatchReceived.selector;
    }

    receive() external payable {}
}