package cl.vc.arb.apps.fh.gui;

import cl.vc.arb.apps.fh.notif.Notification;
import cl.vc.arb.apps.fh.notif.NotificationType;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Toasts apilados en la esquina inferior derecha del frame principal. Maximo {@value #MAX} visibles
 * a la vez (al llegar uno nuevo expira el mas viejo) y autocierre a los {@value #MS} ms. No roban foco.
 */
final class ToastManager {

    private static final int MAX = 3;
    private static final int MS = 4000;
    private static final int W = 330, H = 78, GAP = 10, MARGIN = 18;

    private final JFrame owner;
    private final Deque<JWindow> active = new ArrayDeque<>(); // solo EDT

    ToastManager(JFrame owner) {
        this.owner = owner;
    }

    void show(Notification n) {
        SwingUtilities.invokeLater(() -> {
            while (active.size() >= MAX) {
                JWindow oldest = active.pollFirst();
                if (oldest != null) {
                    oldest.dispose();
                }
            }
            JWindow w = build(n);
            active.addLast(w);
            reposition();
            w.setVisible(true);

            Timer t = new Timer(MS, e -> {
                w.dispose();
                active.remove(w);
                reposition();
            });
            t.setRepeats(false);
            t.start();
        });
    }

    private JWindow build(Notification n) {
        Color accent = colorFor(n.type);

        JWindow w = new JWindow(owner);
        w.setSize(W, H);
        w.setAlwaysOnTop(true);
        w.setFocusableWindowState(false);

        JPanel p = new JPanel(new BorderLayout(10, 0));
        p.setBackground(new Color(0x171e28));
        p.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(new Color(0x2c3744), 1, true),
                BorderFactory.createEmptyBorder(10, 12, 10, 14)));

        JPanel side = new JPanel();
        side.setBackground(accent);
        side.setPreferredSize(new Dimension(4, H));
        p.add(side, BorderLayout.WEST);

        JPanel txt = new JPanel();
        txt.setOpaque(false);
        txt.setLayout(new BoxLayout(txt, BoxLayout.Y_AXIS));

        JLabel title = new JLabel((n.title == null || n.title.isBlank()) ? n.type.name() : n.title);
        title.setForeground(accent);
        title.setFont(title.getFont().deriveFont(Font.BOLD, 13f));
        title.setAlignmentX(Component.LEFT_ALIGNMENT);

        JLabel msg = new JLabel("<html><body style='width:248px'>" + escape(n.message) + "</body></html>");
        msg.setForeground(new Color(0xc8d2dd));
        msg.setFont(msg.getFont().deriveFont(12f));
        msg.setAlignmentX(Component.LEFT_ALIGNMENT);

        txt.add(title);
        txt.add(Box.createVerticalStrut(3));
        txt.add(msg);
        p.add(txt, BorderLayout.CENTER);

        w.setContentPane(p);
        return w;
    }

    /** Reubica los toasts: el mas nuevo pegado abajo, los anteriores apilados hacia arriba. */
    private void reposition() {
        Rectangle b = owner.isShowing()
                ? owner.getBounds()
                : GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        int x = b.x + b.width - W - MARGIN;
        int y = b.y + b.height - MARGIN - H;

        List<JWindow> list = new ArrayList<>(active);
        for (int i = list.size() - 1; i >= 0; i--) {
            list.get(i).setLocation(x, y);
            y -= (H + GAP);
        }
    }

    private static Color colorFor(NotificationType t) {
        switch (t) {
            case CONEXION:
                return new Color(0x3fd35f);
            case DESCONEXION:
            case STOP:
                return new Color(0xff5b52);
            case ACTUALIZACION:
                return new Color(0xfa8c00);
            case LATENCIA:
                return new Color(0xe3b341);
            default:
                return new Color(0x58a6ff);
        }
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
