export interface DetailRowData {
  /** Set when labels alone may collide, such as user-chosen ref names. */
  id?: string;
  label: string;
  value: string;
  isCopyable?: boolean;
  /** Set when the value is a list shown one item per line. */
  items?: string[];
}
