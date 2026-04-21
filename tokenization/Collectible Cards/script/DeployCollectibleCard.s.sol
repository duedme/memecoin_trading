// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.27;

import {Script, console} from "forge-std/Script.sol";
import {AccessManager} from "@openzeppelin/contracts/access/manager/AccessManager.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {CollectibleCard} from "../src/CollectibleCard.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

contract DeployCollectibleCard is Script {
    // Role 0 = ADMIN_ROLE (built-in en AccessManager)
    uint64 public constant CARD_MANAGER_ROLE = 1;
    uint64 public constant DIVIDEND_MANAGER_ROLE = 2;
    uint64 public constant UPGRADER_ROLE = 3;

    function run() external {
        uint256 deployerPrivateKey = vm.envUint("PRIVATE_KEY");
        address deployer = vm.addr(deployerPrivateKey);
        address royaltyReceiver = vm.envOr("ROYALTY_RECEIVER", deployer);
        string memory contractURI = vm.envOr("CONTRACT_URI", string(""));

        vm.startBroadcast(deployerPrivateKey);

        // 1. Deploy AccessManager
        AccessManager manager = new AccessManager(deployer);
        console.log("AccessManager:", address(manager));

        // 2. Deploy implementación
        CollectibleCard implementation = new CollectibleCard();
        console.log("Implementation:", address(implementation));

        // 3. Deploy proxy UUPS
        bytes memory initData =
            abi.encodeCall(CollectibleCard.initialize, (address(manager), royaltyReceiver, contractURI));
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        console.log("Proxy:", address(proxy));

        // 4. Configue which functions can call which role
        bytes4[] memory cardManagerSelectors = new bytes4[](9);
        cardManagerSelectors[0] = CollectibleCard.createCard.selector;
        cardManagerSelectors[1] = CollectibleCard.setTokenURI.selector;
        cardManagerSelectors[2] = CollectibleCard.freezeMetadata.selector;
        cardManagerSelectors[3] = CollectibleCard.setContractURI.selector;
        cardManagerSelectors[4] = CollectibleCard.modifyRoyalty.selector;
        cardManagerSelectors[5] = CollectibleCard.modifyRoyaltyReceiver.selector;
        cardManagerSelectors[6] = CollectibleCard.pause.selector;
        cardManagerSelectors[7] = CollectibleCard.unpause.selector;
        cardManagerSelectors[8] = CollectibleCard.withdraw.selector;

        manager.setTargetFunctionRole(address(proxy), cardManagerSelectors, CARD_MANAGER_ROLE);

        bytes4[] memory dividendSelectors = new bytes4[](1);
        dividendSelectors[0] = CollectibleCard.depositDividends.selector;
        manager.setTargetFunctionRole(address(proxy), dividendSelectors, DIVIDEND_MANAGER_ROLE);

        bytes4[] memory upgradeSelectors = new bytes4[](1);
        upgradeSelectors[0] = UUPSUpgradeable.upgradeToAndCall.selector;
        manager.setTargetFunctionRole(address(proxy), upgradeSelectors, UPGRADER_ROLE);

        // 5. Assign roles to deployer (0 = no delay)
        manager.grantRole(CARD_MANAGER_ROLE, deployer, 0);
        manager.grantRole(DIVIDEND_MANAGER_ROLE, deployer, 0);
        manager.grantRole(UPGRADER_ROLE, deployer, 0);

        manager.labelRole(CARD_MANAGER_ROLE, "CARD_MANAGER");
        manager.labelRole(DIVIDEND_MANAGER_ROLE, "DIVIDEND_MANAGER");
        manager.labelRole(UPGRADER_ROLE, "UPGRADER");

        console.log("Roles configured. Deployer has all roles.");

        vm.stopBroadcast();
    }
}
