import { buttonControlHeight } from "@/components/ui/control-geometry";
import { Shortcut } from "@/components/ui/shortcut";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { Theme } from "@/styles/theme";
import type { ShortcutKey } from "@/utils/format-shortcut";
import { Plus } from "lucide-react-native";
import React from "react";
import { useTranslation } from "react-i18next";
import { Text, View, type LayoutChangeEvent, type PressableStateCallbackType } from "react-native";
import { StyleSheet, withUnistyles } from "react-native-unistyles";

const ThemedPlus = withUnistyles(Plus);
const extraMutedColorMapping = (theme: Theme) => ({
  color: theme.colors.foregroundExtraMuted,
});

interface WorkspaceNewAgentButtonProps {
  shortcutKeys: ShortcutKey[][] | null;
  onCreateAgentTab: () => void;
  onLayout: (event: LayoutChangeEvent) => void;
}

function buttonStyle({ hovered, pressed }: PressableStateCallbackType) {
  return [styles.button, (hovered || pressed) && styles.buttonHovered];
}

export function WorkspaceNewAgentButton({
  shortcutKeys,
  onCreateAgentTab,
  onLayout,
}: WorkspaceNewAgentButtonProps) {
  const { t } = useTranslation();
  const label = t("workspace.tabs.actions.newAgent");

  return (
    <View style={styles.container} onLayout={onLayout}>
      <Tooltip delayDuration={0} enabledOnDesktop enabledOnMobile={false}>
        <TooltipTrigger
          testID="workspace-new-agent-button"
          onPress={onCreateAgentTab}
          accessibilityRole="button"
          accessibilityLabel={label}
          style={buttonStyle}
        >
          <ThemedPlus size={14} uniProps={extraMutedColorMapping} />
        </TooltipTrigger>
        <TooltipContent side="bottom" align="center" offset={8}>
          <View style={styles.tooltipRow}>
            <Text style={styles.tooltipText}>{label}</Text>
            {shortcutKeys ? <Shortcut chord={shortcutKeys} /> : null}
          </View>
        </TooltipContent>
      </Tooltip>
    </View>
  );
}

const styles = StyleSheet.create((theme) => ({
  container: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: theme.spacing[1],
  },
  button: {
    width: buttonControlHeight.xs,
    height: buttonControlHeight.xs,
    borderRadius: theme.borderRadius.md,
    alignItems: "center",
    justifyContent: "center",
    outlineWidth: 0,
    outlineColor: "transparent",
  },
  buttonHovered: {
    backgroundColor: theme.colors.surface2,
  },
  tooltipText: {
    color: theme.colors.foreground,
    fontSize: theme.fontSize.base,
  },
  tooltipRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: theme.spacing[2],
  },
}));
