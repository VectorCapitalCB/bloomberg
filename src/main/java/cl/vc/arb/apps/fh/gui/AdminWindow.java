package cl.vc.arb.apps.fh.gui;

import akka.actor.ActorRef;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.ingest.Bar;
import cl.vc.arb.apps.fh.ingest.BloombergGateway;
import cl.vc.arb.apps.fh.notif.Notification;
import cl.vc.arb.apps.fh.notif.NotificationCenter;
import cl.vc.arb.apps.fh.notif.NotificationType;
import cl.vc.arb.apps.fh.ss.SellSideManager;
import cl.vc.arb.apps.fh.utils.BookSnapshot;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.extern.slf4j.Slf4j;

import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.JTableHeader;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.image.BufferedImage;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Ventana de escritorio (Swing) del panel de administracion. Lee el estado del app directamente
 * (misma JVM, sin HTTP). Tema oscuro intenso con acento naranja. Al cerrar se minimiza a la bandeja.
 */
@Slf4j
public class AdminWindow {

    private static final Color BG = new Color(0x0a0e14), PANEL = new Color(0x11161d), PANEL2 = new Color(0x171e28),
            BD = new Color(0x232f3d), TX = new Color(0xe8eef5), TX2 = new Color(0x93a1b2), TX3 = new Color(0x5d6b7a),
            ACCENT = new Color(0xfa8c00), GREEN = new Color(0x3fd35f), RED = new Color(0xff5b52), AMBER = new Color(0xe3b341),
            ROW_A = new Color(0x11161d), ROW_B = new Color(0x141b25), FBG_UP = new Color(0x123a1d), FBG_DN = new Color(0x44150f);

    private JFrame frame;
    private SecModel model;
    private JTable table;
    private JTextPane logPane;
    private JScrollPane logScroll;
    private JLabel statusPill, lblSub;
    private JTextField symField;
    private final Map<String, JLabel> metric = new LinkedHashMap<>();

    private ChartPanel chart;
    private String chartInterval = "DAILY";
    private volatile String chartKey = "";

    private ToastManager toasts;
    private DefaultListModel<Notification> notifModel;
    private JList<Notification> notifList;

    private JPanel bottomContent;
    private CardLayout bottomCards;
    private boolean bottomExpanded = false;
    private String bottomCurrent = "log";
    private JToggleButton tabLog, tabNotif;

    private long prevTicks = -1, prevTs = 0;
    private final Map<String, double[]> prevVals = new HashMap<>();

    public void show() {
        SwingUtilities.invokeLater(this::build);
    }

    private void build() {
        try {
            com.formdev.flatlaf.FlatDarkLaf.setup();
            UIManager.put("Panel.background", BG);
            UIManager.put("control", BG);
            UIManager.put("Table.background", PANEL);
            UIManager.put("Table.foreground", TX);
            UIManager.put("Table.gridColor", PANEL);
            UIManager.put("Table.selectionBackground", new Color(0x213044));
            UIManager.put("Table.selectionForeground", TX);
            UIManager.put("TableHeader.background", new Color(0x0c1119));
            UIManager.put("TableHeader.foreground", TX2);
            UIManager.put("TableHeader.separatorColor", BD);
            UIManager.put("TableHeader.bottomSeparatorColor", BD);
            UIManager.put("ScrollPane.background", PANEL);
            UIManager.put("Viewport.background", PANEL);
            UIManager.put("ScrollBar.thumb", new Color(0x2a3645));
            UIManager.put("ScrollBar.track", BG);
            UIManager.put("TextField.background", new Color(0x0c1119));
            UIManager.put("TextField.foreground", TX);
            UIManager.put("TextField.borderColor", BD);
            UIManager.put("Button.background", PANEL2);
            UIManager.put("Button.foreground", TX);
            UIManager.put("Button.hoverBackground", new Color(0x222d3a));
            UIManager.put("Button.borderColor", BD);
            UIManager.put("Button.arc", 8);
            UIManager.put("Component.focusColor", ACCENT);
            UIManager.put("Component.focusedBorderColor", ACCENT);
            UIManager.put("Label.foreground", TX);
            UIManager.put("TitledBorder.titleColor", TX2);
        } catch (Throwable ignore) {
        }

        frame = new JFrame("ORB-BLOOMBERG");
        frame.setIconImage(icon(64));
        frame.setSize(1080, 740);
        frame.setMinimumSize(new Dimension(900, 600));
        frame.setLocationRelativeTo(null);
        frame.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
        frame.addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) { onClose(); }
        });

        JPanel root = new JPanel(new BorderLayout(0, 12));
        root.setBackground(BG);
        root.setBorder(BorderFactory.createEmptyBorder(0, 0, 12, 0));

        root.add(buildHeader(), BorderLayout.NORTH);

        JPanel body = new JPanel(new BorderLayout(0, 12));
        body.setBackground(BG);
        body.setBorder(BorderFactory.createEmptyBorder(0, 14, 0, 14));
        body.add(buildCards(), BorderLayout.NORTH);
        body.add(buildCenter(), BorderLayout.CENTER);
        body.add(buildBottom(), BorderLayout.SOUTH);
        root.add(body, BorderLayout.CENTER);

        frame.setContentPane(root);
        frame.setVisible(true);
        setupTray();

        // Toasts abajo-derecha + refresco de la pestaña de notificaciones en cada evento.
        toasts = new ToastManager(frame);
        NotificationCenter.get().addListener(n -> {
            toasts.show(n);
            SwingUtilities.invokeLater(this::refreshNotifs);
        });
        refreshNotifs();

        new Timer(1000, e -> refresh()).start();
        new Timer(2000, e -> refreshLog()).start();
        refresh();
        refreshLog();

        Timer firstChk = new Timer(8000, e -> { ((Timer) e.getSource()).stop(); checkUpdate(false); });
        firstChk.setRepeats(false);
        firstChk.start();
        new Timer(6 * 60 * 60 * 1000, e -> checkUpdate(false)).start();

        log.info("ventana de administracion abierta");
    }

    private JComponent buildHeader() {
        JPanel h = new JPanel(new BorderLayout());
        h.setBackground(PANEL);
        h.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 2, 0, ACCENT),
                BorderFactory.createEmptyBorder(12, 16, 12, 16)));

        JPanel left = new JPanel(new FlowLayout(FlowLayout.LEFT, 11, 0));
        left.setOpaque(false);
        JLabel ico = new JLabel(new ImageIcon(icon(30)));
        JLabel title = new JLabel("ORB-BLOOMBERG");
        title.setForeground(TX);
        title.setFont(title.getFont().deriveFont(Font.BOLD, 17f));
        statusPill = new JLabel("●  iniciando");
        statusPill.setOpaque(true);
        statusPill.setBackground(new Color(0x14241a));
        statusPill.setForeground(GREEN);
        statusPill.setFont(statusPill.getFont().deriveFont(Font.BOLD, 12f));
        statusPill.setBorder(BorderFactory.createEmptyBorder(4, 11, 4, 11));
        left.add(ico);
        left.add(title);
        left.add(statusPill);
        h.add(left, BorderLayout.WEST);

        return h;
    }

    /** Botones de control (reconectar/parar/arrancar/actualizar). Se ubican abajo a la derecha. */
    private JPanel buildControls() {
        JPanel btns = new JPanel(new FlowLayout(FlowLayout.RIGHT, 7, 0));
        btns.setOpaque(false);
        btns.add(btn("reconectar", () -> {
            BloombergGateway g = MainApp.getBloombergGateway();
            NotificationCenter.get().publish(NotificationType.DESCONEXION, "Bloomberg", "reconectando sesión…");
            if (g != null) { g.stop(); g.start(); }
            NotificationCenter.get().publish(NotificationType.CONEXION, "Bloomberg", "sesión restablecida");
        }, "reconectando"));
        btns.add(btn("parar", () -> {
            BloombergGateway g = MainApp.getBloombergGateway();
            if (g != null) g.stop();
            NotificationCenter.get().publish(NotificationType.STOP, "Ingesta", "ingesta detenida por el usuario");
        }, "ingesta detenida"));
        btns.add(btn("arrancar", () -> {
            BloombergGateway g = MainApp.getBloombergGateway();
            if (g != null) g.start();
            NotificationCenter.get().publish(NotificationType.CONEXION, "Ingesta", "ingesta iniciada");
        }, "ingesta iniciada"));
        JButton bUpd = new JButton("actualizar");
        bUpd.addActionListener(e -> checkUpdate(true));
        btns.add(bUpd);
        return btns;
    }

    private JComponent buildCards() {
        JPanel wrap = new JPanel(new BorderLayout(0, 8));
        wrap.setOpaque(false);
        lblSub = new JLabel("…");
        lblSub.setForeground(TX2);
        lblSub.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        wrap.add(lblSub, BorderLayout.NORTH);

        JPanel cards = new JPanel(new GridLayout(1, 6, 10, 0));
        cards.setOpaque(false);
        for (String k : new String[]{"ticks / s", "total ticks", "papeles", "suscriptores", "out writes", "cola"}) {
            cards.add(card(k));
        }
        wrap.add(cards, BorderLayout.CENTER);
        return wrap;
    }

    private JComponent card(String label) {
        JPanel c = new JPanel(new BorderLayout(0, 4));
        c.setBackground(PANEL2);
        c.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(BD, 1, true),
                BorderFactory.createEmptyBorder(9, 12, 10, 12)));
        JLabel l = new JLabel(label.toUpperCase());
        l.setForeground(TX3);
        l.setFont(l.getFont().deriveFont(11f));
        JLabel v = new JLabel("0");
        v.setForeground(TX);
        v.setFont(new Font(Font.MONOSPACED, Font.BOLD, 21));
        metric.put(label, v);
        c.add(l, BorderLayout.NORTH);
        c.add(v, BorderLayout.CENTER);
        return c;
    }

    private JComponent buildCenter() {
        JPanel p = new JPanel(new BorderLayout(0, 8));
        p.setOpaque(false);

        JPanel subRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 7, 0));
        subRow.setOpaque(false);
        JLabel pl = new JLabel("papel:");
        pl.setForeground(TX2);
        symField = new JTextField(28);
        symField.setToolTipText("Ticker Bloomberg completo. Los papeles bootstrap quedan listados aunque aun no llegue feed.");
        symField.addActionListener(e -> doSubscribe());
        JButton bSub = new JButton("+ suscribir"); bSub.addActionListener(e -> doSubscribe());
        JButton bUns = new JButton("desuscribir sel."); bUns.addActionListener(e -> doUnsub());
        subRow.add(pl); subRow.add(symField); subRow.add(bSub); subRow.add(bUns);

        JLabel legend = new JLabel("○ pendiente de feed  • con snapshot");
        legend.setForeground(TX3);
        legend.setBorder(BorderFactory.createEmptyBorder(2, 4, 0, 0));

        JPanel north = new JPanel(new BorderLayout(0, 4));
        north.setOpaque(false);
        north.add(subRow, BorderLayout.NORTH);
        north.add(legend, BorderLayout.SOUTH);
        p.add(north, BorderLayout.NORTH);

        model = new SecModel();
        table = new JTable(model);
        table.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        table.setRowHeight(24);
        table.setShowGrid(false);
        table.setIntercellSpacing(new Dimension(0, 0));
        table.setFillsViewportHeight(true);
        table.setDefaultRenderer(Object.class, new SecRenderer());
        JTableHeader th = table.getTableHeader();
        th.setReorderingAllowed(false);
        th.setFont(th.getFont().deriveFont(Font.BOLD, 11.5f));
        table.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) loadChart();
        });
        JScrollPane sc = new JScrollPane(table);
        sc.setBorder(BorderFactory.createLineBorder(BD, 1, true));
        sc.getViewport().setBackground(PANEL);

        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, sc, buildChart());
        split.setResizeWeight(0.56);
        split.setBorder(null);
        split.setDividerSize(7);
        split.setBackground(BG);
        split.setPreferredSize(new Dimension(1040, 340));
        p.add(split, BorderLayout.CENTER);
        return p;
    }

    private JComponent buildChart() {
        JPanel p = new JPanel(new BorderLayout(0, 0));
        p.setBackground(new Color(0x0d1117));
        p.setBorder(BorderFactory.createLineBorder(BD, 1, true));

        JPanel tb = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 5));
        tb.setBackground(new Color(0x0c1119));
        JLabel t = new JLabel("  gráfico  ");
        t.setForeground(TX2);
        t.setFont(t.getFont().deriveFont(Font.BOLD, 11.5f));
        tb.add(t);
        ButtonGroup grp = new ButtonGroup();
        for (String[] iv : new String[][]{{"1m", "1"}, {"5m", "5"}, {"15m", "15"}, {"1D", "DAILY"}}) {
            JToggleButton tg = new JToggleButton(iv[0]);
            tg.setFocusable(false);
            tg.setFont(tg.getFont().deriveFont(11f));
            if (iv[1].equals(chartInterval)) tg.setSelected(true);
            String code = iv[1];
            tg.addActionListener(e -> { chartInterval = code; chartKey = ""; loadChart(); });
            grp.add(tg);
            tb.add(tg);
        }
        p.add(tb, BorderLayout.NORTH);

        chart = new ChartPanel();
        p.add(chart, BorderLayout.CENTER);
        return p;
    }

    /** Carga el grafico del papel seleccionado (en background; no recarga si no cambió papel+intervalo). */
    private void loadChart() {
        int r = table.getSelectedRow();
        if (r < 0) return;
        Row row = model.rowAt(r);
        String symbol = (row.symbol != null && !row.symbol.isEmpty()) ? row.symbol : row.topic;
        if (symbol == null || symbol.isEmpty()) return;
        String key = symbol + "|" + chartInterval;
        if (key.equals(chartKey)) return;
        chartKey = key;
        final String sym = symbol, iv = chartInterval;
        chart.setStatus(sym + "  ·  " + label(iv), "cargando…");
        new Thread(() -> {
            List<Bar> bars;
            try {
                BloombergGateway g = MainApp.getBloombergGateway();
                boolean sim = "yes".equalsIgnoreCase(MainApp.getProperties().getProperty("simulador", "no"));
                bars = (g != null && g.isConnected() && !sim) ? g.history(sym, iv, 120) : synthBars(sym, iv, 120);
            } catch (Throwable t) {
                bars = new ArrayList<>();
            }
            final List<Bar> fb = bars;
            SwingUtilities.invokeLater(() -> {
                if (!(sym + "|" + iv).equals(chartKey)) return; // la selección ya cambió
                if (fb.isEmpty())
                    chart.setStatus(sym + "  ·  " + label(iv), "sin datos (¿histórico permitido en tu DAPI?)");
                else
                    chart.setData(sym + "  ·  " + label(iv) + "  ·  " + fb.size() + " velas", fb);
            });
        }, "chart-load").start();
    }

    private static String label(String iv) {
        return "DAILY".equalsIgnoreCase(iv) ? "diario" : iv + "m";
    }

    /** Velas sintéticas para modo simulador / sin Bloomberg (random walk sembrado con el last actual). */
    private static List<Bar> synthBars(String symbol, String interval, int n) {
        List<Bar> out = new ArrayList<>();
        double base = 0;
        for (BookSnapshot bs : MainApp.bookHasmap.values()) {
            if (bs != null && symbol.equals(bs.getSymbol())) {
                double l = bs.getStatistic().getLast();
                if (l > 0) base = l;
                break;
            }
        }
        if (base <= 0) base = 50 + Math.abs(symbol.hashCode() % 950);
        double price = base * 0.96;
        long now = System.currentTimeMillis();
        long step = "DAILY".equalsIgnoreCase(interval) ? 86_400_000L : Long.parseLong(interval) * 60_000L;
        double vol = base * 0.006;
        for (int i = 0; i < n; i++) {
            double open = price;
            double close = Math.max(0.0001, open + (Math.random() - 0.48) * vol);
            double high = Math.max(open, close) + Math.random() * vol * 0.8;
            double low = Math.min(open, close) - Math.random() * vol * 0.8;
            double v = 1000 + Math.random() * 9000;
            out.add(new Bar(now - (long) (n - i) * step, open, high, low, close, v));
            price = close;
        }
        return out;
    }

    /**
     * Panel inferior plegable: por defecto solo se ven las dos solapas ("log en vivo" /
     * "notificaciones"). Al hacer clic en una se despliega su contenido; clic en la activa lo contrae.
     */
    private JComponent buildBottom() {
        JPanel container = new JPanel(new BorderLayout(0, 6));
        container.setOpaque(false);

        JPanel header = new JPanel(new BorderLayout());
        header.setOpaque(false);

        JPanel tabsLeft = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 2));
        tabsLeft.setOpaque(false);
        tabLog = makeTab("log en vivo");
        tabNotif = makeTab("notificaciones");
        tabLog.addActionListener(e -> toggleBottom("log"));
        tabNotif.addActionListener(e -> toggleBottom("notif"));
        tabsLeft.add(tabLog);
        tabsLeft.add(tabNotif);

        header.add(tabsLeft, BorderLayout.WEST);
        header.add(buildControls(), BorderLayout.EAST);
        container.add(header, BorderLayout.NORTH);

        bottomCards = new CardLayout();
        bottomContent = new JPanel(bottomCards);
        bottomContent.setOpaque(false);
        bottomContent.add(buildLog(), "log");
        bottomContent.add(buildNotifs(), "notif");
        bottomContent.setVisible(false); // arranca contraido
        container.add(bottomContent, BorderLayout.CENTER);

        return container;
    }

    private JToggleButton makeTab(String text) {
        JToggleButton b = new JToggleButton("▸  " + text);
        b.setFocusable(false);
        b.setFont(b.getFont().deriveFont(Font.BOLD, 12f));
        return b;
    }

    /** Despliega/contrae el cajon inferior y refresca el estado visual de las solapas. */
    private void toggleBottom(String card) {
        if (bottomExpanded && bottomCurrent.equals(card)) {
            bottomExpanded = false;
            bottomContent.setVisible(false);
        } else {
            bottomExpanded = true;
            bottomCurrent = card;
            bottomCards.show(bottomContent, card);
            bottomContent.setVisible(true);
        }
        boolean logOpen = bottomExpanded && bottomCurrent.equals("log");
        boolean notifOpen = bottomExpanded && bottomCurrent.equals("notif");
        tabLog.setSelected(logOpen);
        tabNotif.setSelected(notifOpen);
        tabLog.setText((logOpen ? "▾  " : "▸  ") + "log en vivo");
        tabNotif.setText((notifOpen ? "▾  " : "▸  ") + "notificaciones");
        if (frame != null) {
            frame.revalidate();
            frame.repaint();
        }
    }

    private JComponent buildNotifs() {
        notifModel = new DefaultListModel<>();
        notifList = new JList<>(notifModel);
        notifList.setBackground(new Color(0x0d1117));
        notifList.setFixedCellHeight(40);
        notifList.setCellRenderer(new NotifRenderer());

        JScrollPane sc = new JScrollPane(notifList);
        sc.getViewport().setBackground(new Color(0x0d1117));
        sc.setBorder(BorderFactory.createLineBorder(BD, 1, true));
        sc.setPreferredSize(new Dimension(1040, 200));

        JPanel p = new JPanel(new BorderLayout(0, 6));
        p.setOpaque(false);

        JPanel bar = new JPanel(new BorderLayout());
        bar.setOpaque(false);
        JLabel t = new JLabel("historial (persistido en Redis)");
        t.setForeground(TX2);
        JButton bClear = new JButton("limpiar vista");
        bClear.addActionListener(e -> { if (notifModel != null) notifModel.clear(); });
        bar.add(t, BorderLayout.WEST);
        JPanel rb = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        rb.setOpaque(false);
        rb.add(bClear);
        bar.add(rb, BorderLayout.EAST);

        p.add(bar, BorderLayout.NORTH);
        p.add(sc, BorderLayout.CENTER);
        return p;
    }

    private void refreshNotifs() {
        if (notifModel == null) return;
        notifModel.clear();
        for (Notification n : NotificationCenter.get().snapshot()) {
            notifModel.addElement(n);
        }
    }

    private static Color notifColor(NotificationType t) {
        switch (t) {
            case CONEXION: return new Color(0x3fd35f);
            case DESCONEXION:
            case STOP: return new Color(0xff5b52);
            case ACTUALIZACION: return new Color(0xfa8c00);
            case LATENCIA: return new Color(0xe3b341);
            default: return new Color(0x58a6ff);
        }
    }

    final class NotifRenderer extends DefaultListCellRenderer {
        private final java.text.SimpleDateFormat fmt = new java.text.SimpleDateFormat("HH:mm:ss");

        @Override
        public Component getListCellRendererComponent(JList<?> list, Object value, int index,
                                                      boolean sel, boolean foc) {
            super.getListCellRendererComponent(list, value, index, sel, foc);
            Notification n = (Notification) value;
            String color = String.format("%06x", notifColor(n.type).getRGB() & 0xFFFFFF);
            String time = fmt.format(new java.util.Date(n.ts));
            setText("<html><span style='color:#" + color + "'>● " + n.type + "</span>&nbsp;&nbsp;<b>"
                    + esc(n.title) + "</b> — " + esc(n.message)
                    + "&nbsp;&nbsp;<span style='color:#5d6b7a'>" + time + "</span></html>");
            setBorder(BorderFactory.createEmptyBorder(4, 12, 4, 12));
            setForeground(TX);
            setBackground(sel ? new Color(0x213044) : (index % 2 == 0 ? ROW_A : ROW_B));
            return this;
        }

        private String esc(String s) {
            if (s == null) return "";
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        }
    }

    private JComponent buildLog() {
        JPanel p = new JPanel(new BorderLayout(0, 6));
        p.setOpaque(false);

        JPanel bar = new JPanel(new BorderLayout());
        bar.setOpaque(false);
        JLabel t = new JLabel("log en vivo");
        t.setForeground(TX2);
        JPanel rb = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        rb.setOpaque(false);
        JButton bCopy = new JButton("copiar todo");
        bCopy.addActionListener(e -> {
            copyLog();
            bCopy.setText("✓ copiado");
            Timer tt = new Timer(1500, a -> bCopy.setText("copiar todo"));
            tt.setRepeats(false);
            tt.start();
        });
        JButton bFolder = new JButton("abrir carpeta");
        bFolder.addActionListener(e -> openLogFolder());
        rb.add(bCopy);
        rb.add(bFolder);
        bar.add(t, BorderLayout.WEST);
        bar.add(rb, BorderLayout.EAST);
        p.add(bar, BorderLayout.NORTH);

        logPane = new JTextPane();
        logPane.setEditable(false);
        logPane.setBackground(new Color(0x0d1117));
        logPane.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        logScroll = new JScrollPane(logPane);
        logScroll.getViewport().setBackground(new Color(0x0d1117));
        logScroll.setPreferredSize(new Dimension(1040, 200));
        logScroll.setBorder(BorderFactory.createLineBorder(BD, 1, true));
        p.add(logScroll, BorderLayout.CENTER);
        return p;
    }

    private JButton btn(String text, Runnable action, String okMsg) {
        JButton b = new JButton(text);
        b.addActionListener(e -> new Thread(() -> {
            try { action.run(); log.info("admin gui: {}", okMsg); }
            catch (Exception ex) { log.warn("admin gui action error", ex); }
        }).start());
        return b;
    }

    private void refresh() {
        try {
            boolean sim = "yes".equalsIgnoreCase(MainApp.getProperties().getProperty("simulador", "no"));
            BloombergGateway g = MainApp.getBloombergGateway();
            boolean conn = sim || (g != null && g.isConnected());
            statusPill.setText("●  " + (conn ? (sim ? "simulador" : "conectado") : "desconectado"));
            statusPill.setForeground(conn ? GREEN : RED);
            statusPill.setBackground(conn ? new Color(0x14241a) : new Color(0x2a1413));

            String host = MainApp.getProperties().getProperty("bloomberg.host", "localhost");
            String port = MainApp.getProperties().getProperty("bloomberg.port", "8194");
            lblSub.setText("Bloomberg " + host + ":" + port + (sim ? "  ·  SIMULADOR" : "")
                    + "      clientes proto → " + nettyEndpoint()
                    + "      uptime " + uptime(System.currentTimeMillis() - MainApp.getStartTimeMs()));

            long ticks = MainApp.getBloombergTicks().sum();
            long now = System.currentTimeMillis(), tps = 0;
            if (prevTicks >= 0 && now > prevTs) tps = Math.max(0, (ticks - prevTicks) * 1000 / (now - prevTs));
            prevTicks = ticks; prevTs = now;
            setMetric("ticks / s", String.valueOf(tps));
            setMetric("total ticks", num(ticks));
            setMetric("papeles", String.valueOf(MainApp.bookHasmap.size()));
            setMetric("suscriptores", String.valueOf(MainApp.getActiveTopicSubscribers().sum()));
            setMetric("out writes", num(MainApp.getOutboundWrites().sum()));
            setMetric("cola", String.valueOf(MainApp.queued()));

            String selTopic = selectedTopic();
            model.update(buildRows());
            reselect(selTopic);
        } catch (Exception ignore) {
        }
    }

    private void setMetric(String k, String v) {
        JLabel l = metric.get(k);
        if (l != null) l.setText(v);
    }

    private List<Row> buildRows() {
        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, BookSnapshot> e : MainApp.bookHasmap.entrySet()) {
            BookSnapshot bs = e.getValue();
            if (bs == null) continue;
            MarketDataMessage.Statistic.Builder st = bs.getStatistic();
            Row r = new Row();
            r.topic = e.getKey();
            r.symbol = bs.getSymbol();
            r.bid = st.getBidPx(); r.bidSize = st.getBidQty();
            r.ask = st.getAskPx(); r.askSize = st.getAskQty();
            r.last = st.getLast(); r.vol = st.getVolume();
            r.ready = Boolean.TRUE.equals(bs.getReceivedSnapshot());
            double[] prev = prevVals.get(r.topic);
            if (prev != null) {
                r.flashBid = prev[0] != r.bid;
                r.flashAsk = prev[1] != r.ask;
                r.flashLast = prev[2] != r.last;
                r.lastDir = Double.compare(r.last, prev[2]);
            }
            prevVals.put(r.topic, new double[]{r.bid, r.ask, r.last});
            rows.add(r);
        }
        return rows;
    }

    private void refreshLog() {
        try {
            String dir = System.getProperty("log.dir", MainApp.getProperties().getProperty("path.logs", "logs"));
            Path p = Paths.get(dir, "orb-bloomberg.log");
            if (!Files.exists(p)) return;
            JScrollBar vsb = logScroll.getVerticalScrollBar();
            boolean atBottom = vsb.getValue() + vsb.getVisibleAmount() >= vsb.getMaximum() - 45;
            StyledDocument doc = logPane.getStyledDocument();
            doc.remove(0, doc.getLength());
            for (String line : tail(p, 220, 128 * 1024).split("\n")) {
                doc.insertString(doc.getLength(), line + "\n", styleFor(line));
            }
            if (atBottom) logPane.setCaretPosition(doc.getLength());
        } catch (Exception ignore) {
        }
    }

    private SimpleAttributeSet styleFor(String line) {
        SimpleAttributeSet s = new SimpleAttributeSet();
        Color c = new Color(0x76838f);
        if (line.contains("CONECTADO") || line.contains("DATA OK") || line.contains("session OK") || line.contains("ServiceOpened"))
            c = new Color(0x5f9e6c);
        else if (line.contains("[WARN]") || line.contains("Failure") || line.contains("FALLO") || line.contains("AVISO"))
            c = new Color(0xab8736);
        else if (line.contains("[ERROR]") || line.contains("Exception") || line.contains("BindException"))
            c = new Color(0xb85a52);
        StyleConstants.setForeground(s, c);
        return s;
    }

    private void copyLog() {
        try {
            String dir = System.getProperty("log.dir", MainApp.getProperties().getProperty("path.logs", "logs"));
            Path p = Paths.get(dir, "orb-bloomberg.log");
            String text = Files.exists(p) ? tail(p, 50000, 4 * 1024 * 1024) : "(sin log)";
            Toolkit.getDefaultToolkit().getSystemClipboard()
                    .setContents(new java.awt.datatransfer.StringSelection(text), null);
            log.info("admin gui: log copiado al portapapeles ({} chars)", text.length());
        } catch (Exception ex) {
            log.warn("admin gui copyLog error", ex);
        }
    }

    private void openLogFolder() {
        try {
            String dir = System.getProperty("log.dir", MainApp.getProperties().getProperty("path.logs", "logs"));
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().open(new java.io.File(dir));
            }
        } catch (Exception ex) {
            log.warn("admin gui openLogFolder error", ex);
        }
    }

    // -------- auto-update --------

    private void checkUpdate(boolean manual) {
        if (!manual && !Boolean.parseBoolean(MainApp.getProperties().getProperty("update.enabled", "true"))) return;
        new Thread(() -> {
            String url = MainApp.getProperties().getProperty("update.url", "http://172.16.0.8:8060");
            String nv = cl.vc.arb.apps.fh.update.Updater.checkLatest(url);
            SwingUtilities.invokeLater(() -> {
                if (nv == null) {
                    if (manual) info("Estás en la última versión (" + cl.vc.arb.apps.fh.update.Updater.current() + ").");
                    return;
                }
                NotificationCenter.get().publish(NotificationType.ACTUALIZACION, "Actualización",
                        "versión nueva disponible: " + nv + " (actual " + cl.vc.arb.apps.fh.update.Updater.current() + ")");
                int r = JOptionPane.showConfirmDialog(frame,
                        "Hay una versión nueva: " + nv + "   (tienes " + cl.vc.arb.apps.fh.update.Updater.current() + ").\n"
                                + "Se descargará, se cerrará el app y se reabrirá actualizado.\n¿Actualizar ahora?",
                        "Actualización disponible", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE);
                if (r == JOptionPane.YES_OPTION) doUpdate(url, nv);
            });
        }).start();
    }

    private void doUpdate(String url, String version) {
        JDialog d = new JDialog(frame, "Actualizando", false);
        JPanel pp = new JPanel(new BorderLayout(10, 12));
        pp.setBorder(BorderFactory.createEmptyBorder(18, 20, 18, 20));
        pp.add(new JLabel("Descargando ORB-BLOOMBERG " + version + " …"), BorderLayout.NORTH);
        JProgressBar pb = new JProgressBar();
        pb.setIndeterminate(true);
        pp.add(pb, BorderLayout.CENTER);
        d.setContentPane(pp);
        d.pack();
        d.setLocationRelativeTo(frame);
        d.setVisible(true);
        new Thread(() -> {
            try {
                java.nio.file.Path msi = cl.vc.arb.apps.fh.update.Updater.download(url, version);
                SwingUtilities.invokeLater(d::dispose);
                cl.vc.arb.apps.fh.update.Updater.applyAndExit(msi);
            } catch (Exception ex) {
                SwingUtilities.invokeLater(() -> {
                    d.dispose();
                    error("No se pudo actualizar: " + ex.getMessage());
                });
            }
        }).start();
    }

    private void info(String m) {
        JOptionPane.showMessageDialog(frame, m, "ORB-BLOOMBERG", JOptionPane.INFORMATION_MESSAGE);
    }

    private void error(String m) {
        JOptionPane.showMessageDialog(frame, m, "ORB-BLOOMBERG", JOptionPane.ERROR_MESSAGE);
    }

    private void doSubscribe() {
        String s = symField.getText().trim();
        if (s.isEmpty()) return;
        try {
            MarketDataMessage.Subscribe sub = MarketDataMessage.Subscribe.newBuilder()
                    .setSymbol(s).setId("gui-" + s)
                    .setSecurityExchange(MainApp.securityExchange)
                    .setSettlType(RoutingMessage.SettlType.REGULAR)
                    .setTrade(true).setStatistic(true).setBook(true)
                    .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK).build();
            MainApp.getSellSideManager().tell(new SellSideManager.Subscribe(sub, ActorRef.noSender()), ActorRef.noSender());
            log.info("admin gui: suscripcion manual symbol='{}'", s);
            symField.setText("");
        } catch (Exception ex) {
            log.warn("admin gui subscribe error", ex);
        }
    }

    private void doUnsub() {
        int r = table.getSelectedRow();
        if (r < 0) return;
        Row row = model.rowAt(r);
        if (row.topic == null) return;
        MainApp.getSellSideManager().tell(new SellSideManager.AdminUnsub(row.topic), ActorRef.noSender());
        MainApp.bookHasmap.remove(row.topic);
        log.info("admin gui: desuscripcion topic='{}'", row.topic);
    }

    private String selectedTopic() {
        int r = table.getSelectedRow();
        return r >= 0 ? model.rowAt(r).topic : null;
    }

    private void reselect(String topic) {
        if (topic == null) return;
        for (int i = 0; i < model.getRowCount(); i++) {
            if (topic.equals(model.rowAt(i).topic)) {
                table.setRowSelectionInterval(i, i);
                return;
            }
        }
    }

    private void onClose() {
        if (SystemTray.isSupported()) {
            Object[] options = {"Minimizar", "Salir"};
            int choice = JOptionPane.showOptionDialog(frame,
                    "ORB-BLOOMBERG sigue corriendo en segundo plano para mantener el redistribuidor activo.\n"
                            + "Puedes minimizarlo a la bandeja o cerrarlo por completo.",
                    "Cerrar ORB-BLOOMBERG",
                    JOptionPane.DEFAULT_OPTION,
                    JOptionPane.QUESTION_MESSAGE,
                    null,
                    options,
                    options[0]);
            if (choice == 1) {
                System.exit(0);
            }
            if (choice == 0 || choice == JOptionPane.CLOSED_OPTION) {
                frame.setVisible(false);
            }
        } else if (JOptionPane.showConfirmDialog(frame,
                "¿Cerrar ORB-BLOOMBERG? Se detiene el redistribuidor.", "Salir",
                JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
            System.exit(0);
        }
    }

    private void setupTray() {
        if (!SystemTray.isSupported()) return;
        try {
            PopupMenu pm = new PopupMenu();
            MenuItem open = new MenuItem("Abrir panel");
            open.addActionListener(e -> restore());
            MenuItem quit = new MenuItem("Salir");
            quit.addActionListener(e -> System.exit(0));
            pm.add(open); pm.addSeparator(); pm.add(quit);
            TrayIcon ti = new TrayIcon(icon(16), "ORB-BLOOMBERG", pm);
            ti.setImageAutoSize(true);
            ti.addActionListener(e -> restore());
            SystemTray.getSystemTray().add(ti);
        } catch (Exception ex) {
            log.debug("tray no disponible", ex);
        }
    }

    private void restore() {
        frame.setVisible(true);
        frame.setExtendedState(Frame.NORMAL);
        frame.toFront();
        frame.requestFocus();
    }

    private String nettyEndpoint() {
        String sh = MainApp.getProperties().getProperty("server.host", "");
        String port = sh.contains(":") ? sh.substring(sh.indexOf(':') + 1) : sh;
        String ip = "localhost";
        try { ip = java.net.InetAddress.getLocalHost().getHostAddress(); } catch (Exception ignore) { }
        return ip + ":" + port;
    }

    // -------- model + renderer --------

    static final class Row {
        String topic, symbol;
        double bid, bidSize, ask, askSize, last, vol;
        boolean ready, flashBid, flashAsk, flashLast;
        int lastDir;
    }

    static final class SecModel extends AbstractTableModel {
        private final String[] cols = {"símbolo", "bid", "b.sz", "ask", "a.sz", "spread", "last", "vol", "●"};
        private List<Row> rows = new ArrayList<>();
        void update(List<Row> r) { this.rows = r; fireTableDataChanged(); }
        Row rowAt(int i) { return (i >= 0 && i < rows.size()) ? rows.get(i) : new Row(); }
        public int getRowCount() { return rows.size(); }
        public int getColumnCount() { return cols.length; }
        public String getColumnName(int c) { return cols[c]; }
        public boolean isCellEditable(int r, int c) { return false; }
        public Object getValueAt(int r, int c) {
            Row x = rows.get(r);
            switch (c) {
                case 0: return x.symbol != null && !x.symbol.isEmpty() ? x.symbol : x.topic;
                case 1: return px(x.bid);
                case 2: return sz(x.bidSize);
                case 3: return px(x.ask);
                case 4: return sz(x.askSize);
                case 5: return (x.ask > 0 && x.bid > 0) ? px(x.ask - x.bid) : "—";
                case 6: return px(x.last);
                case 7: return sz(x.vol);
                case 8: return x.ready ? "●" : "○";
                default: return "";
            }
        }
    }

    final class SecRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable t, Object v, boolean sel, boolean foc, int row, int col) {
            super.getTableCellRendererComponent(t, v, sel, foc, row, col);
            setHorizontalAlignment(col == 0 ? LEFT : RIGHT);
            setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 10));
            Row r = model.rowAt(row);
            Color fg = TX, bgc = (row % 2 == 0) ? ROW_A : ROW_B;
            if (col == 1) { fg = GREEN; if (r.flashBid) bgc = FBG_UP; }
            else if (col == 3) { fg = RED; if (r.flashAsk) bgc = FBG_DN; }
            else if (col == 6) { fg = r.lastDir >= 0 ? GREEN : RED; if (r.flashLast) bgc = r.lastDir >= 0 ? FBG_UP : FBG_DN; }
            else if (col == 0) { fg = TX; setFont(getFont().deriveFont(Font.BOLD)); }
            else if (col == 2 || col == 4 || col == 5) fg = TX3;
            else if (col == 8) fg = r.ready ? GREEN : TX3;
            if (sel) {
                setBackground(new Color(0x213044));
                setForeground(col == 1 ? GREEN : col == 3 ? RED : TX);
            } else {
                setBackground(bgc);
                setForeground(fg);
            }
            return this;
        }
    }

    // -------- grafico de velas --------

    /** Panel que dibuja un candlestick OHLC con volumen, eje de precio a la derecha y linea de last. */
    static final class ChartPanel extends JComponent {
        private List<Bar> bars = new ArrayList<>();
        private String title = "selecciona un papel";
        private String status = "haz clic en un papel de la tabla";

        void setData(String title, List<Bar> bars) {
            this.title = title;
            this.bars = (bars != null) ? bars : new ArrayList<>();
            this.status = "";
            repaint();
        }

        void setStatus(String title, String status) {
            this.title = title;
            this.status = status;
            this.bars = new ArrayList<>();
            repaint();
        }

        @Override
        protected void paintComponent(Graphics g0) {
            Graphics2D g = (Graphics2D) g0;
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            int w = getWidth(), h = getHeight();
            g.setColor(new Color(0x0d1117));
            g.fillRect(0, 0, w, h);

            g.setColor(TX2);
            g.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 12));
            g.drawString(title, 10, 18);

            if (bars.isEmpty()) {
                g.setColor(TX3);
                g.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
                String msg = status.isEmpty() ? "—" : status;
                FontMetrics fm = g.getFontMetrics();
                g.drawString(msg, Math.max(10, (w - fm.stringWidth(msg)) / 2), h / 2);
                return;
            }

            int padR = 56, padB = 16, plotL = 8;
            int top = 28, plotR = w - padR;
            int volH = (int) ((h - top - padB) * 0.18);
            int gap = 6;
            int priceBot = h - padB - volH - gap;
            int volTop = h - padB - volH, volBot = h - padB;
            int plotW = Math.max(10, plotR - plotL);

            double min = Double.MAX_VALUE, max = -Double.MAX_VALUE, maxVol = 0;
            for (Bar b : bars) {
                if (!Double.isNaN(b.low)) min = Math.min(min, b.low);
                if (!Double.isNaN(b.high)) max = Math.max(max, b.high);
                if (!Double.isNaN(b.volume)) maxVol = Math.max(maxVol, b.volume);
            }
            if (min == Double.MAX_VALUE) { min = 0; max = 1; }
            if (max <= min) max = min + 1;
            double range = max - min;
            min -= range * 0.04; max += range * 0.04; range = max - min;

            // gridlines + etiquetas de precio (derecha)
            g.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 10));
            int lines = 4;
            for (int i = 0; i <= lines; i++) {
                double pr = max - range * i / lines;
                int y = top + (priceBot - top) * i / lines;
                g.setColor(new Color(0x1a2330));
                g.drawLine(plotL, y, plotR, y);
                g.setColor(TX3);
                g.drawString(fmtPx(pr), plotR + 4, y + 3);
            }

            int n = bars.size();
            double slot = (double) plotW / n;
            int cw = (int) Math.max(1, Math.min(14, slot * 0.62));

            for (int i = 0; i < n; i++) {
                Bar b = bars.get(i);
                int cx = (int) (plotL + slot * (i + 0.5));
                boolean up = b.close >= b.open;
                g.setColor(up ? GREEN : RED);
                int yHigh = py(b.high, max, range, top, priceBot);
                int yLow = py(b.low, max, range, top, priceBot);
                int yOpen = py(b.open, max, range, top, priceBot);
                int yClose = py(b.close, max, range, top, priceBot);
                g.drawLine(cx, yHigh, cx, yLow);
                int bodyTop = Math.min(yOpen, yClose), bodyH = Math.max(2, Math.abs(yClose - yOpen));
                g.fillRect(cx - cw / 2, bodyTop, Math.max(1, cw), bodyH);
                if (maxVol > 0 && !Double.isNaN(b.volume)) {
                    int vh = (int) ((b.volume / maxVol) * (volBot - volTop));
                    g.setColor(new Color(up ? 0x1f6b34 : 0x6b241f));
                    g.fillRect(cx - cw / 2, volBot - vh, Math.max(1, cw), vh);
                }
            }

            // linea + etiqueta del ultimo cierre
            Bar last = bars.get(n - 1);
            int yl = py(last.close, max, range, top, priceBot);
            g.setColor(ACCENT);
            g.drawLine(plotL, yl, plotR, yl);
            g.fillRect(plotR, yl - 7, padR, 14);
            g.setColor(Color.BLACK);
            g.setFont(new Font(Font.MONOSPACED, Font.BOLD, 10));
            g.drawString(fmtPx(last.close), plotR + 3, yl + 3);
        }

        private static int py(double v, double max, double range, int top, int bot) {
            if (Double.isNaN(v)) v = max;
            double frac = (max - v) / range;
            return (int) (top + frac * (bot - top));
        }

        private static String fmtPx(double v) {
            if (Double.isNaN(v)) return "";
            double a = Math.abs(v);
            if (a >= 1000) return String.format("%,.0f", v);
            if (a >= 100) return String.format("%.2f", v);
            return String.format("%.4f", v);
        }
    }

    // -------- helpers --------

    static Image icon(int sz) {
        BufferedImage img = new BufferedImage(sz, sz, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setColor(new Color(250, 140, 0));
        int r = Math.max(2, sz / 5);
        g.fillRoundRect(0, 0, sz, sz, r * 2, r * 2);
        g.setColor(Color.WHITE);
        g.setFont(new Font("Arial", Font.BOLD, (int) (sz * 0.64)));
        FontMetrics fm = g.getFontMetrics();
        int x = (sz - fm.stringWidth("B")) / 2;
        int y = (sz - fm.getHeight()) / 2 + fm.getAscent();
        g.drawString("B", x, y);
        g.dispose();
        return img;
    }

    static String uptime(long ms) {
        long s = ms / 1000;
        return String.format("%02d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60);
    }

    static String num(long n) {
        if (Math.abs(n) >= 1_000_000) return String.format("%.2fM", n / 1e6);
        if (Math.abs(n) >= 1_000) return String.format("%.1fK", n / 1e3);
        return String.valueOf(n);
    }

    static String px(double n) {
        if (n == 0 || Double.isNaN(n)) return "—";
        return Math.abs(n) >= 100 ? String.format("%.2f", n) : trim(String.format("%.4f", n));
    }

    static String trim(String s) {
        if (s.contains(".")) {
            s = s.replaceAll("0+$", "");
            if (s.endsWith(".")) s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    static String sz(double n) {
        if (n == 0 || Double.isNaN(n)) return "—";
        double a = Math.abs(n);
        if (a >= 1e6) return String.format("%.2fM", n / 1e6);
        if (a >= 1e3) return String.format("%.1fK", n / 1e3);
        return String.valueOf((long) n);
    }

    static String tail(Path file, int lines, int maxBytes) {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            long len = raf.length();
            long start = Math.max(0, len - maxBytes);
            raf.seek(start);
            byte[] buf = new byte[(int) (len - start)];
            raf.readFully(buf);
            String text = new String(buf, StandardCharsets.UTF_8);
            if (start > 0) {
                int nl = text.indexOf('\n');
                if (nl >= 0) text = text.substring(nl + 1);
            }
            String[] arr = text.split("\n", -1);
            int from = Math.max(0, arr.length - lines);
            StringBuilder sb = new StringBuilder();
            for (int i = from; i < arr.length; i++) {
                if (i > from) sb.append('\n');
                sb.append(arr[i]);
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
