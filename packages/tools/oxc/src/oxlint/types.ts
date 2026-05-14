export type Range = [number, number]

export interface BaseNode {
  type: string
  range: Range
}

export namespace ESTree {
  export interface Identifier extends BaseNode {
    type: "Identifier"
    name: string
  }

  export interface StringLiteral extends BaseNode {
    type: "Literal"
    value: string
  }

  export interface MemberExpression extends BaseNode {
    type: "MemberExpression"
    object: Expression
    property: Expression
  }

  export interface CallExpression extends BaseNode {
    type: "CallExpression"
    callee: Expression
  }

  export type Expression = Identifier | StringLiteral | MemberExpression | CallExpression

  export interface ImportSpecifier extends BaseNode {
    type: "ImportSpecifier"
    imported: Identifier | StringLiteral
    local: Identifier
    importKind?: "value" | "type"
  }

  export interface ImportDefaultSpecifier extends BaseNode {
    type: "ImportDefaultSpecifier"
    local: Identifier
  }

  export interface ImportNamespaceSpecifier extends BaseNode {
    type: "ImportNamespaceSpecifier"
    local: Identifier
  }

  export type ImportDeclarationSpecifier =
    | ImportSpecifier
    | ImportDefaultSpecifier
    | ImportNamespaceSpecifier

  export interface ImportDeclaration extends BaseNode {
    type: "ImportDeclaration"
    specifiers: Array<ImportDeclarationSpecifier>
    source: StringLiteral
    importKind?: "value" | "type"
  }

  export interface ExportAllDeclaration extends BaseNode {
    type: "ExportAllDeclaration"
    source: StringLiteral
  }

  export interface ExportNamedDeclaration extends BaseNode {
    type: "ExportNamedDeclaration"
    source: StringLiteral | null
  }

  export interface PropertyDefinition extends BaseNode {
    type: "PropertyDefinition"
    static: boolean
  }

  export interface MethodDefinition extends BaseNode {
    type: "MethodDefinition"
    static: boolean
  }

  export interface ClassElement extends BaseNode {
    static?: boolean
  }

  export interface ClassBody extends BaseNode {
    type: "ClassBody"
    body: Array<ClassElement>
  }

  export interface Class extends BaseNode {
    type: "ClassDeclaration" | "ClassExpression"
    superClass: Expression | null
    body: ClassBody
  }
}

export interface Fix {
  range: Range
  text: string
}

export interface Fixer {
  insertTextBefore(node: { range: Range }, text: string): Fix
  insertTextBeforeRange(range: Range, text: string): Fix
  insertTextAfter(node: { range: Range }, text: string): Fix
  insertTextAfterRange(range: Range, text: string): Fix
  remove(node: { range: Range }): Fix
  removeRange(range: Range): Fix
  replaceText(node: { range: Range }, text: string): Fix
  replaceTextRange(range: Range, text: string): Fix
}

export type Visitor = {
  [key: string]: ((node: never) => void) | undefined
}

export interface RuleMeta {
  type?: "problem" | "suggestion" | "layout"
  docs?: { description?: string }
  schema?: ReadonlyArray<unknown>
  fixable?: "code" | "whitespace"
}

export interface ReportDescriptor {
  node: unknown
  message: string
  fix?: (fixer: Fixer) => Fix | Array<Fix> | null
}

export interface Context {
  id: string
  filename: string
  physicalFilename?: string
  options: ReadonlyArray<unknown>
  report(diagnostic: ReportDescriptor): void
}

export interface CreateRule {
  meta?: RuleMeta
  create(context: Context): Visitor
}
