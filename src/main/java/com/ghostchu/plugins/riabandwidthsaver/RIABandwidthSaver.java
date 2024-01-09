package com.ghostchu.plugins.riabandwidthsaver;

import com.comphenix.protocol.PacketType;
import com.comphenix.protocol.ProtocolLibrary;
import com.comphenix.protocol.events.*;
import com.comphenix.protocol.injector.temporary.TemporaryPlayer;
import com.ghostchu.plugins.riabandwidthsaver.hooks.cmi.CMIHook;
import com.ghostchu.plugins.riabandwidthsaver.hooks.essx.ESSXHook;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class RIABandwidthSaver extends JavaPlugin implements Listener {
    private final Set<UUID> AFK_PLAYERS = new HashSet<>();
    private final Map<PacketType, PacketInfo> PKT_TYPE_STATS = new ConcurrentHashMap<>();
    private final Map<UUID, PacketInfo> PLAYER_PKT_SAVED_STATS = new ConcurrentHashMap<>();
    private final Map<PacketType, PacketInfo> UNFILTERED_PKT_TYPE_STATS = new ConcurrentHashMap<>();
    private final Map<UUID, PacketInfo> UNFILTERED_PLAYER_PKT_SAVED_STATS = new ConcurrentHashMap<>();
    private final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
    private boolean calcAllPackets = false;
    private final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    private final List<AFKHook> afkHooks = new ArrayList<>();

    @Override
    public void onEnable() {
        // Plugin startup logic
        saveDefaultConfig();
        Bukkit.getPluginManager().registerEvents(this, this);
        reloadConfig();
        scanHooks();
    }

    private void scanHooks() {
        if(Bukkit.getPluginManager().getPlugin("CMI") != null){
            afkHooks.add(new CMIHook(this));
            getLogger().info("CMI AFK状态钩子已注册！");
        }
        if(Bukkit.getPluginManager().getPlugin("Essentials")  != null){
            afkHooks.add(new ESSXHook(this));
            getLogger().info("Essentials AFK状态钩子已注册！");
        }
        if(afkHooks.size() > 1){
            getLogger().warning("存在多个 AFK 状态源钩子，这可能会导致问题。请只选择一个使用，并关闭其它插件的 AFK 功能以规避问题，如已关闭可忽略此提示");
        }
        if(afkHooks.isEmpty()){
            getLogger().severe("未检测到任何支持的 AFK 状态钩子，插件退出……");
            Bukkit.getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void reloadConfig() {
        super.reloadConfig();
        this.calcAllPackets = getConfig().getBoolean("calcAllPackets", true);
        ProtocolLibrary.getProtocolManager().removePacketListeners(this);
        initProtocolLib();
    }

    private void initProtocolLib() {
        if (calcAllPackets) {
            List<PacketType> types = new ArrayList<>();
            for (PacketType value : PacketType.values()) {
                if (value.isServer() && value.isSupported()) {
                    types.add(value);
                }
            }
            ProtocolLibrary.getProtocolManager().addPacketListener(new PacketAdapter(
                    this,
                    ListenerPriority.MONITOR,
                    types, ListenerOptions.ASYNC) {
                @Override
                public void onPacketSending(PacketEvent event) {
                    PacketContainer container = event.getPacket().shallowClone();
                    CompletableFuture.runAsync(() -> {
                        long packetSize = getPacketSize(container);
                        UNFILTERED_PKT_TYPE_STATS.compute(event.getPacketType(), (k, v) -> {
                            if (v == null) {
                                v = new PacketInfo();
                            }
                            v.getPktCounter().increment();
                            v.getPktSize().add(packetSize);
                            return v;
                        });
                        if (!(event.getPlayer() instanceof TemporaryPlayer)) {
                            UNFILTERED_PLAYER_PKT_SAVED_STATS.compute(event.getPlayer().getUniqueId(), (k, v) -> {
                                if (v == null) {
                                    v = new PacketInfo();
                                }
                                v.getPktCounter().increment();
                                v.getPktSize().add(packetSize);
                                return v;
                            });
                        }
                    }, EXECUTOR_SERVICE);
                }
            });
        } else {
            UNFILTERED_PLAYER_PKT_SAVED_STATS.clear();
            UNFILTERED_PKT_TYPE_STATS.clear();
        }
        ProtocolLibrary.getProtocolManager().addPacketListener(new PacketAdapter(
                this,
                ListenerPriority.HIGHEST,
                Stream.of(
                        PacketType.Play.Server.ANIMATION,
                        PacketType.Play.Server.BLOCK_BREAK_ANIMATION,
                        PacketType.Play.Server.ENTITY_SOUND,
                        PacketType.Play.Server.NAMED_SOUND_EFFECT,
                        PacketType.Play.Server.WORLD_PARTICLES,
                        PacketType.Play.Server.EXPLOSION,
                        PacketType.Play.Server.UPDATE_TIME,
                        PacketType.Play.Server.ENTITY_HEAD_ROTATION,
                        PacketType.Play.Server.HURT_ANIMATION,
                        PacketType.Play.Server.DAMAGE_EVENT,
                        PacketType.Play.Server.ENTITY_LOOK,
                        PacketType.Play.Server.REL_ENTITY_MOVE,
                        PacketType.Play.Server.REL_ENTITY_MOVE_LOOK,
                        PacketType.Play.Server.SPAWN_ENTITY_EXPERIENCE_ORB,
                        PacketType.Play.Server.VEHICLE_MOVE,
                        PacketType.Play.Server.BLOCK_ACTION,
                        PacketType.Play.Server.LIGHT_UPDATE,
                        PacketType.Play.Server.LOOK_AT,
                        PacketType.Play.Server.PLAYER_LIST_HEADER_FOOTER,
                        PacketType.Play.Server.WORLD_EVENT,
                        PacketType.Play.Server.COLLECT,
                        PacketType.Play.Server.ENTITY_EFFECT).filter(PacketType::isSupported).collect(Collectors.toList()), ListenerOptions.ASYNC) {
            @Override
            public void onPacketSending(PacketEvent event) {
                UUID uuid = event.getPlayer().getUniqueId();
                if (!AFK_PLAYERS.contains(uuid)) {
                    return;
                }
                PacketType type = event.getPacketType();
                if (type == PacketType.Play.Server.REL_ENTITY_MOVE
                        || type == PacketType.Play.Server.VEHICLE_MOVE
                        || type == PacketType.Play.Server.REL_ENTITY_MOVE_LOOK
                        || type == PacketType.Play.Server.SPAWN_ENTITY_EXPERIENCE_ORB) {
                    if (RANDOM.nextInt(3) > 0) {
                        return;
                    }
                }
                if (type == PacketType.Play.Server.SET_SLOT) {
                    Byte windowId = event.getPacket().getBytes().readSafely(0);
                    if (windowId != null && windowId.intValue() == 0) {
                        return;
                    }
                }
                event.setCancelled(true);
                PacketContainer container = event.getPacket().shallowClone();
                CompletableFuture.runAsync(() -> {
                    long packetSize = getPacketSize(container);
                    PKT_TYPE_STATS.compute(event.getPacketType(), (k, v) -> {
                        if (v == null) {
                            v = new PacketInfo();
                        }
                        v.getPktCounter().increment();
                        v.getPktSize().add(packetSize);
                        return v;
                    });
                    if (!(event.getPlayer() instanceof TemporaryPlayer)) {
                        PLAYER_PKT_SAVED_STATS.compute(event.getPlayer().getUniqueId(), (k, v) -> {
                            if (v == null) {
                                v = new PacketInfo();
                            }
                            v.getPktCounter().increment();
                            v.getPktSize().add(packetSize);
                            return v;
                        });
                    }
                }, EXECUTOR_SERVICE);
            }
        });
    }

    private long getPacketSize(PacketContainer packetContainer) {
        try {
            ByteBuf buffer = (ByteBuf) packetContainer.serializeToBuffer();
            if (buffer != null) {
                long size = buffer.readableBytes();
                ReferenceCountUtil.safeRelease(buffer);
                return size;
            } else {
                return 0L;
            }
        } catch (Exception e) {
            return -1L;
        }

    }

    public void playerEcoEnable(Player player) {
        String message = getConfig().getString("message.playerEcoEnable", "");
        if(!message.isEmpty()){
            player.sendMessage(message);
        }
        player.setSendViewDistance(2);
        AFK_PLAYERS.add(player.getUniqueId());
    }

    public void playerEcoDisable(Player player) {
        AFK_PLAYERS.remove(player.getUniqueId());
        player.setSendViewDistance(-1);
        player.resetPlayerTime();
        String message = getConfig().getString("message.playerEcoDisable", "");
        if(!message.isEmpty()){
            player.sendMessage(message);
        }
    }

    @EventHandler(ignoreCancelled = true, priority = EventPriority.MONITOR)
    public void onPlayerQuit(PlayerQuitEvent event) {
        playerEcoDisable(event.getPlayer());
        PLAYER_PKT_SAVED_STATS.remove(event.getPlayer().getUniqueId());
        UNFILTERED_PLAYER_PKT_SAVED_STATS.remove(event.getPlayer().getUniqueId());
    }

    @Override
    public void onDisable() {
        // Plugin shutdown logic
        ProtocolLibrary.getProtocolManager().removePacketListeners(this);
    }



    @Override
    public List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (args.length == 1) {
            return  List.of(
                    "reload",
                    "unfiltered"
            );
        }
        return null;
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (args.length == 0) {
            sender.sendMessage(ChatColor.GREEN + "🍃 ECO 节能模式 - 统计信息：");
            long pktCancelled = PKT_TYPE_STATS.values().stream().mapToLong(r -> r.getPktCounter().longValue()).sum();
            long pktSizeSaved = PKT_TYPE_STATS.values().stream().mapToLong(r -> r.getPktSize().longValue()).sum();
            sender.sendMessage(ChatColor.YELLOW + "共减少发送数据包：" + ChatColor.AQUA + pktCancelled + " 个");
            sender.sendMessage(ChatColor.YELLOW + "共减少发送数据包：" + ChatColor.AQUA + humanReadableByteCount(pktSizeSaved, false) + " （不包含视距优化的增益数据）");
            Map<PacketType, PacketInfo> sortedPktMap = new LinkedHashMap<>();
            Map<UUID, PacketInfo> sortedPlayerMap = new LinkedHashMap<>();
            PKT_TYPE_STATS.entrySet().stream().sorted(Map.Entry.<PacketType, PacketInfo>comparingByValue().reversed()).forEachOrdered(e -> sortedPktMap.put(e.getKey(), e.getValue()));
            PLAYER_PKT_SAVED_STATS.entrySet().stream().sorted(Map.Entry.<UUID, PacketInfo>comparingByValue().reversed()).forEachOrdered(e -> sortedPlayerMap.put(e.getKey(), e.getValue()));
            sender.sendMessage(ChatColor.YELLOW + " -- 数据包类型节约 TOP 5 --");
            sortedPktMap.entrySet().stream().limit(5).forEach(entry -> sender.sendMessage(ChatColor.GRAY + entry.getKey().name() + " - " + humanReadableByteCount(entry.getValue().getPktSize().longValue(), false)));
            sender.sendMessage(ChatColor.YELLOW + " -- 玩家流量节约 TOP 5 --");
            sortedPlayerMap.entrySet().stream().limit(5).forEach(entry -> sender.sendMessage(ChatColor.GRAY + Bukkit.getOfflinePlayer(entry.getKey()).getName() + " - " + humanReadableByteCount(entry.getValue().getPktSize().longValue(), false)));
        }
        if (args.length == 1 && args[0].equalsIgnoreCase("unfiltered")) {
            sender.sendMessage(ChatColor.GREEN + "🍃 UN-ECO - 数据总计 - 统计信息：");
            long pktSent = UNFILTERED_PKT_TYPE_STATS.values().stream().mapToLong(r -> r.getPktCounter().longValue()).sum();
            long pktSize = UNFILTERED_PKT_TYPE_STATS.values().stream().mapToLong(r -> r.getPktSize().longValue()).sum();
            sender.sendMessage(ChatColor.YELLOW + "共发送数据包：" + ChatColor.AQUA + pktSent + " 个");
            sender.sendMessage(ChatColor.YELLOW + "共发送数据包：" + ChatColor.AQUA + humanReadableByteCount(pktSize, false));
            Map<PacketType, PacketInfo> sortedPktMap = new LinkedHashMap<>();
            Map<UUID, PacketInfo> sortedPlayerMap = new LinkedHashMap<>();
            UNFILTERED_PKT_TYPE_STATS.entrySet().stream().sorted(Map.Entry.<PacketType, PacketInfo>comparingByValue().reversed()).forEachOrdered(e -> sortedPktMap.put(e.getKey(), e.getValue()));
            UNFILTERED_PLAYER_PKT_SAVED_STATS.entrySet().stream().sorted(Map.Entry.<UUID, PacketInfo>comparingByValue().reversed()).forEachOrdered(e -> sortedPlayerMap.put(e.getKey(), e.getValue()));
            sender.sendMessage(ChatColor.YELLOW + " -- 数据包类型 TOP 15 --");
            sortedPktMap.entrySet().stream().limit(15).forEach(entry -> sender.sendMessage(ChatColor.GRAY + entry.getKey().name() + " - " + humanReadableByteCount(entry.getValue().getPktSize().longValue(), false)));
            sender.sendMessage(ChatColor.YELLOW + " -- 玩家流量 TOP 5 --");
            sortedPlayerMap.entrySet().stream().limit(5).forEach(entry -> sender.sendMessage(ChatColor.GRAY + Bukkit.getOfflinePlayer(entry.getKey()).getName() + " - " + humanReadableByteCount(entry.getValue().getPktSize().longValue(), false)));
        }
        if (args.length == 1 && args[0].equalsIgnoreCase("reload") && sender.hasPermission("riabandwidthsaver.reload")) {
            reloadConfig();
            sender.sendMessage(ChatColor.GREEN + "🍃 ECO - 配置文件已重载 - RIA.RED - Maintained by Ghost_chu");
        }
        return true;
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
