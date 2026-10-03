package api

// schema is the public GraphQL contract. Roles: a viewer can read everything
// except raw message bodies; an operator can also replay and summarise.
const schema = `
schema {
  query: Query
  mutation: Mutation
}

scalar Time

type Query {
  me: Me!
  aiEnabled: Boolean!
  queues: [Queue!]!
  queue(name: String!): Queue
  group(queue: String!, key: String!): Group
  replayJobs(queue: String!, "Defaults to 20, at most 200." limit: Int): [ReplayJob!]!
  replayJob(id: ID!): ReplayJob
  audit(queue: String, "Defaults to 50, at most 200." limit: Int): [AuditEntry!]!
}

type Mutation {
  "Replay a group. With dryRun the job only reports what it would do. Operators only."
  startReplay(input: ReplayInput!): ReplayJob!
  "Stop a running replay. Operators only."
  cancelReplay(id: ID!): Boolean!
  "Generate a plain-language summary of a group. Operators only; needs AI to be enabled."
  summarizeGroup(queue: String!, key: String!): Group!
}

input ReplayInput {
  queue: String!
  group: String!
  dryRun: Boolean!
  ratePerSecond: Float!
  maxMessages: Int!
}

type Me {
  name: String!
  role: String!
}

type Depth {
  visible: Int!
  inFlight: Int!
  delayed: Int!
}

type ScanStatus {
  at: Time!
  seen: Int!
  new: Int!
  error: String
  skipped: Boolean!
}

type Queue {
  name: String!
  "Live depth of the dead-letter queue, or null when the queue service cannot be reached."
  depth: Depth
  "Live depth of the destination queue."
  destDepth: Depth
  totalPending: Int!
  groups: [Group!]!
  lastScan: ScanStatus
}

type Group {
  queue: String!
  key: String!
  errorSignature: String!
  shape: String!
  count: Int!
  pending: Int!
  firstSeen: Time!
  lastSeen: Time!
  aiSummary: String
  aiSummaryAt: Time
  "pendingOnly defaults to true, limit to 20 (at most 200), offset to 0."
  messages(pendingOnly: Boolean, limit: Int, offset: Int): [Message!]!
}

type Message {
  id: String!
  status: String!
  receiveCount: Int!
  sentAt: Time
  ingestedAt: Time!
  replayedAt: Time
  attributes: [Attribute!]!
  "Raw payload. Null for viewers."
  body: String
}

type Attribute {
  name: String!
  dataType: String!
  value: String!
}

type ReplayJob {
  id: ID!
  queue: String!
  group: String!
  dryRun: Boolean!
  status: String!
  requestedBy: String!
  ratePerSecond: Float!
  maxMessages: Int!
  total: Int!
  replayed: Int!
  skipped: Int!
  failed: Int!
  error: String
  createdAt: Time!
  finishedAt: Time
}

type AuditEntry {
  id: ID!
  at: Time!
  actor: String!
  action: String!
  queue: String!
  group: String
  jobId: String
  detail: String!
}
`
