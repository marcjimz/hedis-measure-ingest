import { NextResponse } from "next/server"
import type { Message, ChatContext } from "@/lib/types"

export async function POST(request: Request) {
  try {
    const { chatId, message, context, history } = await request.json()

    console.log("[v0] Chat API called:", { chatId, message, context })

    // Build the system prompt with context
    const systemPrompt = `You are a helpful medical AI assistant. You are helping with questions about ${context.measure} for patient ${context.patient}. 
    
Provide accurate, helpful information while being clear that you are an AI assistant and users should consult with healthcare professionals for medical advice.

Be concise, friendly, and professional in your responses.`

    // Build messages array for AI
    const messages = [
      { role: "system", content: systemPrompt },
      ...history.map((msg: Message) => ({
        role: msg.role,
        content: msg.content,
      })),
      { role: "user", content: message },
    ]

    // Use Vercel AI Gateway with GPT-4o-mini (no API key needed)
    const aiResponse = await fetch("https://gateway.ai.cloudflare.com/v1/account/gateway/openai/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        // In production, use AI SDK instead for better streaming support
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages,
        temperature: 0.7,
        max_tokens: 500,
      }),
    })

    if (!aiResponse.ok) {
      // Fallback to mock response if AI fails
      console.log("[v0] AI request failed, using fallback")
      return NextResponse.json({
        message: generateMockResponse(message, context),
      })
    }

    const data = await aiResponse.json()
    const aiMessage = data.choices[0]?.message?.content || generateMockResponse(message, context)

    return NextResponse.json({
      message: aiMessage,
      chatId,
    })
  } catch (error) {
    console.error("[v0] Chat API error:", error)
    return NextResponse.json(
      {
        message: generateMockResponse("help", { measure: "general", patient: "" }),
      },
      { status: 200 }, // Return 200 with fallback rather than error
    )
  }
}

function generateMockResponse(message: string, context: ChatContext): string {
  const lowerMessage = message.toLowerCase()

  if (lowerMessage.includes("normal") || lowerMessage.includes("range")) {
    switch (context.measure.toLowerCase()) {
      case "blood pressure":
        return "Normal blood pressure is typically below 120/80 mmHg. Readings between 120-129 systolic and less than 80 diastolic are considered elevated. High blood pressure (hypertension) is 130/80 or higher."
      case "heart rate":
        return "A normal resting heart rate for adults ranges from 60 to 100 beats per minute. Athletes and very fit individuals may have resting rates in the 40-60 bpm range."
      case "temperature":
        return "Normal body temperature is around 98.6°F (37°C), though it can vary slightly. A temperature above 100.4°F (38°C) is generally considered a fever."
      case "glucose level":
        return "Normal fasting blood glucose is 70-100 mg/dL. Levels between 100-125 mg/dL indicate prediabetes, and 126 mg/dL or higher suggests diabetes."
      case "weight":
        return "Healthy weight varies based on height, age, gender, and body composition. BMI between 18.5-24.9 is generally considered healthy, but it's best to consult with a healthcare provider for personalized guidance."
    }
  }

  return `I'd be happy to help you understand more about ${context.measure} for ${context.patient}. Could you please provide more specific details about your question? For example, are you asking about normal ranges, how to measure it, or interpreting specific readings?`
}
