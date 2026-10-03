const copyTextWithSelection = (text: string): boolean => {
  const previouslyFocusedElement = document.activeElement;
  const textArea = document.createElement("textarea");
  textArea.value = text;
  textArea.setAttribute("readonly", "");
  textArea.style.position = "fixed";
  textArea.style.top = "0";
  textArea.style.left = "0";
  textArea.style.opacity = "0";
  document.body.append(textArea);
  textArea.focus({ preventScroll: true });
  textArea.select();
  textArea.setSelectionRange(0, text.length);

  try {
    // eslint-disable-next-line @typescript-eslint/no-deprecated -- Clipboard API is unavailable on insecure origins; execCommand is the only fallback that copies there
    return document.execCommand("copy");
  } catch {
    return false;
  } finally {
    textArea.remove();
    if (previouslyFocusedElement instanceof HTMLElement) {
      previouslyFocusedElement.focus({ preventScroll: true });
    }
  }
};

export const copyText = async (text: string): Promise<boolean> => {
  if (window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      return copyTextWithSelection(text);
    }
  }
  return copyTextWithSelection(text);
};
