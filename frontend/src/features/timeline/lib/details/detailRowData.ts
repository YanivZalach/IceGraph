export interface DetailRowData {
  /** Set when labels alone may collide, such as user-chosen ref names. */
  id?: string;
  label: string;
  value: string;
  isCopyable?: boolean;
  items?: string[];
}
