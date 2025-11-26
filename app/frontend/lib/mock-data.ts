// Mock data for development
import type { User, Chat, ReviewRequest, Measure, NCQAMeasure, Patient } from "./types"

export const mockMeasures: Measure[] = [
  { id: "1", name: "Blood Pressure", description: "Systolic and diastolic measurements" },
  { id: "2", name: "Heart Rate", description: "Beats per minute" },
  { id: "3", name: "Temperature", description: "Body temperature in Fahrenheit" },
  { id: "4", name: "Glucose Level", description: "Blood glucose measurement" },
  { id: "5", name: "Weight", description: "Body weight in pounds" },
]

export const mockCurrentUser: User = {
  id: "user-1",
  name: "John Doe",
  email: "john.doe@example.com",
  role: "reviewer", // Changed from "user" to "reviewer" to enable access to review functionality
}

export const mockChats: Chat[] = [
  {
    id: "chat-1",
    userId: "user-1",
    context: {
      patient: "Jane Smith",
      measure: "Blood Pressure",
    },
    messages: [
      {
        id: "msg-1",
        role: "user",
        content: "What is the normal range for blood pressure?",
        timestamp: new Date("2025-01-15T10:00:00"),
      },
      {
        id: "msg-2",
        role: "assistant",
        content:
          "Normal blood pressure is typically below 120/80 mmHg. Values between 120-129 systolic and less than 80 diastolic are considered elevated.",
        timestamp: new Date("2025-01-15T10:00:05"),
        feedback: "thumbs-up",
      },
    ],
    createdAt: new Date("2025-01-15T10:00:00"),
    updatedAt: new Date("2025-01-15T10:00:05"),
  },
  {
    id: "chat-2",
    userId: "user-1",
    context: {
      patient: "Robert Johnson",
      measure: "Heart Rate",
    },
    messages: [
      {
        id: "msg-3",
        role: "user",
        content: "Is a resting heart rate of 95 normal?",
        timestamp: new Date("2025-01-16T14:30:00"),
      },
      {
        id: "msg-4",
        role: "assistant",
        content:
          "A resting heart rate of 95 bpm is slightly elevated. Normal resting heart rate for adults ranges from 60-100 bpm, but ideally should be between 60-80 bpm.",
        timestamp: new Date("2025-01-16T14:30:05"),
        feedback: "thumbs-down",
      },
    ],
    createdAt: new Date("2025-01-16T14:30:00"),
    updatedAt: new Date("2025-01-16T14:30:05"),
    reviewRequestId: "review-1",
    reviewStatus: "Under Review",
  },
  {
    id: "chat-3",
    userId: "user-1",
    context: {
      patient: "Maria Garcia",
      measure: "Glucose Level",
    },
    messages: [
      {
        id: "msg-5",
        role: "user",
        content: "What is a normal fasting glucose level?",
        timestamp: new Date("2025-01-14T09:15:00"),
      },
      {
        id: "msg-6",
        role: "assistant",
        content: "Normal fasting glucose levels are typically 70-100 mg/dL.",
        timestamp: new Date("2025-01-14T09:15:05"),
      },
    ],
    createdAt: new Date("2025-01-14T09:15:00"),
    updatedAt: new Date("2025-01-17T11:00:00"),
    reviewRequestId: "review-2",
    reviewStatus: "Completed",
  },
  {
    id: "chat-4",
    userId: "user-1",
    context: {
      patient: "Sarah Williams",
      measure: "Temperature",
    },
    messages: [
      {
        id: "msg-7",
        role: "user",
        content: "Is 99.5°F considered a fever?",
        timestamp: new Date("2025-01-13T16:45:00"),
      },
      {
        id: "msg-8",
        role: "assistant",
        content: "A temperature of 99.5°F is slightly elevated but not typically considered a fever.",
        timestamp: new Date("2025-01-13T16:45:05"),
      },
    ],
    createdAt: new Date("2025-01-13T16:45:00"),
    updatedAt: new Date("2025-01-17T14:30:00"),
    reviewRequestId: "review-3",
    reviewStatus: "Returned",
  },
]

export const mockReviewRequests: ReviewRequest[] = [
  {
    id: "review-1",
    chatId: "chat-2",
    userId: "user-1",
    status: "Pending Review",
    createdAt: new Date("2025-01-16T14:35:00"),
    updatedAt: new Date("2025-01-16T14:35:00"),
    priority: 1,
    aiSuggestedAnswer:
      "A resting heart rate of 95 bpm is within the normal range but on the higher end. For most adults, a healthy resting heart rate is 60-100 bpm. Consider factors like fitness level, medications, and recent activity. If consistently elevated, consult with a healthcare provider.",
    aiSupportingContext:
      "Based on clinical guidelines from the American Heart Association, resting heart rate can vary based on age, fitness level, and overall health status.",
  },
  {
    id: "review-2",
    chatId: "chat-3",
    userId: "user-2",
    status: "Under Review",
    createdAt: new Date("2025-01-14T09:15:00"),
    updatedAt: new Date("2025-01-17T11:00:00"),
    assignedTo: "reviewer-1",
    priority: 2,
    aiSuggestedAnswer:
      "Normal fasting glucose levels are typically 70-100 mg/dL. A level of 125 mg/dL indicates prediabetes or diabetes.",
    aiSupportingContext:
      "American Diabetes Association guidelines define fasting glucose levels and their clinical significance.",
  },
  {
    id: "review-3",
    chatId: "chat-4",
    userId: "user-1",
    status: "Returned",
    createdAt: new Date("2025-01-13T16:45:00"),
    updatedAt: new Date("2025-01-17T14:30:00"),
    assignedTo: "reviewer-1",
    priority: 3,
    aiSuggestedAnswer: "A temperature of 99.5°F is slightly elevated but not typically considered a fever.",
    aiSupportingContext: "Based on common medical knowledge, a fever is usually considered to be 100.4°F or higher.",
  },
]

export const mockNCQAMeasures: NCQAMeasure[] = [
  {
    specifications:
      "Breast Cancer Screening (BCS): The percentage of women 50-74 years of age who had a mammogram to screen for breast cancer in the past 27 months.",
    measure: "BCS - Breast Cancer Screening",
    initial_pop: "Women 52-74 years of age with a visit during the measurement period",
    denominator: [
      "Women 50-74 years of age",
      "Enrolled in health plan for continuous period",
      "At least one outpatient visit during measurement period",
    ],
    numerator: [
      "One or more mammograms during the measurement period or the 15 months prior to the measurement period",
    ],
    exclusion: [
      "Women who had a bilateral mastectomy",
      "Women with two unilateral mastectomies",
      "Women with absence of left and right breasts",
    ],
    effective_year: 2025,
    version: "v1.0",
  },
  {
    specifications:
      "Colorectal Cancer Screening (COL): The percentage of adults 50-75 years of age who had appropriate screening for colorectal cancer.",
    measure: "COL - Colorectal Cancer Screening",
    initial_pop: "Adults 51-75 years of age with a visit during the measurement period",
    denominator: [
      "Adults 50-75 years of age",
      "Enrolled in health plan during measurement period",
      "At least one outpatient visit during measurement period",
    ],
    numerator: [
      "Fecal occult blood test (FOBT) during the measurement year",
      "Flexible sigmoidoscopy during the measurement period or the four years prior",
      "Colonoscopy during the measurement period or the nine years prior",
      "CT colonography during the measurement period or the four years prior",
      "FIT-DNA test during the measurement period or the two years prior",
    ],
    exclusion: ["Colorectal cancer diagnosis", "Total colectomy", "Terminal illness", "Advanced illness and frailty"],
    effective_year: 2025,
    version: "v1.0",
  },
  {
    specifications:
      "Comprehensive Diabetes Care (CDC): The percentage of members 18-75 years of age with diabetes (type 1 and type 2) who had each of the following: HbA1c testing, HbA1c poor control, HbA1c control, eye exam, blood pressure control.",
    measure: "CDC - Comprehensive Diabetes Care",
    initial_pop: "Members 18-75 years of age with diabetes during the measurement period",
    denominator: [
      "Members 18-75 years of age",
      "Diagnosis of diabetes (type 1 or type 2)",
      "Enrolled in health plan during measurement period",
    ],
    numerator: [
      "HbA1c testing performed",
      "HbA1c level <8.0% (good control)",
      "HbA1c level >9.0% (poor control)",
      "Eye exam performed by eye care professional",
      "Most recent blood pressure <140/90 mmHg",
      "Medical attention for nephropathy",
    ],
    exclusion: [
      "Members with polycystic ovarian syndrome, gestational diabetes, or steroid-induced diabetes without Type 1 or Type 2 diagnosis",
      "Members in hospice",
      "Members with advanced illness and frailty",
    ],
    effective_year: 2025,
    version: "v2.1",
  },
  {
    specifications:
      "Controlling High Blood Pressure (CBP): The percentage of members 18-85 years of age who had a diagnosis of hypertension and whose blood pressure was adequately controlled during the measurement year.",
    measure: "CBP - Controlling High Blood Pressure",
    initial_pop: "Members 18-85 years of age with hypertension diagnosis",
    denominator: [
      "Members 18-85 years of age",
      "Diagnosis of essential hypertension",
      "At least one outpatient encounter during measurement period",
      "Enrolled in health plan during measurement period",
    ],
    numerator: ["Most recent blood pressure reading during the measurement year is <140/90 mmHg"],
    exclusion: [
      "Evidence of end-stage renal disease",
      "Kidney transplant",
      "Pregnancy",
      "Members in hospice or receiving palliative care",
    ],
    effective_year: 2024,
    version: "v1.5",
  },
  {
    specifications:
      "Hemoglobin A1c Control for Patients With Diabetes (HBD): The percentage of members 18-75 years of age with diabetes (type 1 and type 2) whose most recent HbA1c level during the measurement year is >9.0% (poor control).",
    measure: "HBD - HbA1c Poor Control (>9.0%)",
    initial_pop: "Members 18-75 years of age with diabetes",
    denominator: [
      "Members 18-75 years of age",
      "Diagnosis of diabetes (type 1 or type 2)",
      "Enrolled continuously during measurement period",
    ],
    numerator: [
      "Members whose most recent HbA1c level during the measurement year is >9.0% or was missing a result, or did not have an HbA1c test during the measurement year",
    ],
    exclusion: [
      "Diagnosis of polycystic ovarian syndrome or gestational diabetes",
      "Members in hospice",
      "Members with advanced illness and frailty",
    ],
    effective_year: 2024,
    version: "v1.2",
  },
  {
    specifications:
      "Breast Cancer Screening (BCS) 2024: The percentage of women 50-74 years of age who had a mammogram to screen for breast cancer.",
    measure: "BCS - Breast Cancer Screening",
    initial_pop: "Women 52-74 years of age with a visit during the measurement period",
    denominator: [
      "Women 50-74 years of age",
      "Enrolled in health plan for continuous period",
      "At least one outpatient visit during measurement period",
    ],
    numerator: ["One or more mammograms during the measurement period or the 15 months prior"],
    exclusion: ["Women who had a bilateral mastectomy", "Women with two unilateral mastectomies"],
    effective_year: 2024,
    version: "v1.0",
  },
  {
    specifications:
      "Comprehensive Diabetes Care (CDC) 2024: Diabetes care quality measures including HbA1c testing and control.",
    measure: "CDC - Comprehensive Diabetes Care",
    initial_pop: "Members 18-75 years of age with diabetes during the measurement period",
    denominator: ["Members 18-75 years of age", "Diagnosis of diabetes (type 1 or type 2)"],
    numerator: ["HbA1c testing performed", "HbA1c level <8.0% (good control)", "Eye exam performed"],
    exclusion: ["Members with gestational diabetes only", "Members in hospice"],
    effective_year: 2024,
    version: "v2.0",
  },
  {
    specifications: "Colorectal Cancer Screening (COL) 2023: Screening for colorectal cancer in adults 50-75 years.",
    measure: "COL - Colorectal Cancer Screening",
    initial_pop: "Adults 51-75 years of age",
    denominator: ["Adults 50-75 years of age", "Enrolled in health plan during measurement period"],
    numerator: ["FOBT during measurement year", "Colonoscopy during measurement period or nine years prior"],
    exclusion: ["Colorectal cancer diagnosis", "Total colectomy"],
    effective_year: 2023,
    version: "v1.0",
  },
]

export const mockPatients: Patient[] = [
  {
    id: "pt-1",
    name: "Jane Smith",
    dateOfBirth: "1965-03-15",
    memberId: "MEM001234",
    status: "active",
  },
  {
    id: "pt-2",
    name: "Robert Johnson",
    dateOfBirth: "1958-07-22",
    memberId: "MEM001235",
    status: "active",
  },
  {
    id: "pt-3",
    name: "Maria Garcia",
    dateOfBirth: "1972-11-08",
    memberId: "MEM001236",
    status: "active",
  },
  {
    id: "pt-4",
    name: "Michael Chen",
    dateOfBirth: "1960-05-30",
    memberId: "MEM001237",
    status: "active",
  },
  {
    id: "pt-5",
    name: "Sarah Williams",
    dateOfBirth: "1968-09-12",
    memberId: "MEM001238",
    status: "active",
  },
  {
    id: "pt-6",
    name: "David Brown",
    dateOfBirth: "1955-01-25",
    memberId: "MEM001239",
    status: "inactive",
  },
]
